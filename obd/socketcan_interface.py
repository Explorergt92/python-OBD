import logging
import time
from typing import List, Dict, Optional

try:
    import can  # type: ignore
    import isotp  # type: ignore
except Exception:  # pragma: no cover - optional deps
    can = None
    isotp = None

from .utils import OBDStatus
from .protocols.protocol import Message, ECU

logger = logging.getLogger(__name__)


class SocketCANInterface:
    """SocketCAN + ISO-TP backend to bypass ELM327.

    Features:
    - Functional broadcast (0x7DF) with multi-ECU capture (0x7E8-0x7EF)
    - Physical single ECU requests (default engine 0x7E0/0x7E8)
    - ISO-TP reassembly via python-isotp (multi-frame responses, VIN, etc.)

    Strategy:
      For each request we send a functional frame (unless physical=True) and
      sniff all reply CAN frames for a short capture window, assembling ISO-TP
      per responding ECU. We then emit python-OBD Message objects directly.

    Limitations / Notes:
      - Only classic 11-bit IDs for now. 29-bit could be added with a flag.
      - We hand craft simple ISO-TP assembly for multiple responders because
        python-isotp SocketConnection is 1:1 address pair oriented.
      - Flow control (FC) frames are generated implicitly when sending by
        using isotp for the *physical* connection. For functional broadcast
        we rely on the fact that typical Mode 01 PID replies are <= 7 bytes
        OR that each ECU will respond with its own first frame small enough.
        A future enhancement could create per-ECU dynamic isotp connections
        after sniffing a first frame requiring CFs.
    """

    FUNCTIONAL_REQ_ID = 0x7DF
    ENGINE_REQ_ID = 0x7E0
    ENGINE_RSP_ID = 0x7E8
    MIN_RSP_ID = 0x7E8
    MAX_RSP_ID = 0x7EF

    def __init__(
        self,
        channel: str = "can0",
        bitrate: Optional[int] = None,  # bitrate is configured outside (ip link)
        timeout: float = 0.4,
        capture_window: float = 0.12,
        max_frames: int = 64,
        extended: bool = False,
    ) -> None:
        if can is None:
            raise RuntimeError("python-can is not installed. Install with obd[socketcan].")
        self.channel = channel
        self.timeout = timeout
        self.capture_window = capture_window
        self.max_frames = max_frames
        self.extended = extended
        self._status = OBDStatus.CAR_CONNECTED  # assume bus is configured
        self.bus = can.interface.Bus(channel=channel, bustype="socketcan")

        # Physical ISO-TP session to engine ECU for long replies (VIN, etc.)
        if isotp is not None:
            addr = isotp.Address(
                isotp.AddressingMode.Normal_11bits,
                txid=self.ENGINE_REQ_ID,
                rxid=self.ENGINE_RSP_ID,
            )
            try:
                self.engine_conn = isotp.SocketConnection(
                    self.bus, address=addr, params={"tx_padding": 0x00}
                )
            except Exception:
                self.engine_conn = None
        else:
            self.engine_conn = None

    # -------------- metadata API expected by OBD --------------
    def status(self):
        return self._status

    def port_name(self):
        return self.channel

    def protocol_name(self):  # mimic standard CAN 11/500 (user supplies bitrate)
        return "ISO 15765-4 (SocketCAN 11-bit)"

    def protocol_id(self):
        return "6"  # reuse ID for 11-bit 500k for compatibility

    # -------------- public helpers --------------
    def close(self):
        try:
            if self.engine_conn is not None:
                self.engine_conn.close()
        except Exception:
            pass
        try:
            self.bus.shutdown()
        except Exception:
            pass
        self._status = OBDStatus.NOT_CONNECTED

    # -------------- core integration --------------
    def send_and_parse(self, cmd: bytes, *, physical: bool = False) -> List[Message]:
        """Send an OBD request and capture multi-ECU ISO-TP responses.

        cmd: ASCII hex like b'010C'. Returns list of Message objects.
        """
        if self._status != OBDStatus.CAR_CONNECTED:
            return []

        hex_str = cmd.decode().replace(" ", "")
        try:
            payload = bytes.fromhex(hex_str)
        except ValueError:
            logger.error("Invalid hex command: %s", cmd)
            return []

        if physical and self.engine_conn is not None:
            # Use ISO-TP library for potentially multi-frame response
            try:
                self.engine_conn.send(payload)
                resp = self.engine_conn.recv(timeout=self.timeout)
                if resp:
                    return [self._wrap_payload(resp, ecu=ECU.ENGINE)]
            except Exception as e:
                logger.debug("physical request failed: %s", e)
            return []

        # Functional broadcast: send raw CAN data frame (single or first frame)
        if can is None:
            logger.error("python-can not available at runtime")
            return []
        try:
            out_frame = can.Message(
                arbitration_id=self.FUNCTIONAL_REQ_ID,
                data=payload,
                is_extended_id=self.extended,
            )
            self.bus.send(out_frame)
        except Exception as e:
            logger.error("Failed to send CAN frame: %s", e)
            return []

        deadline = time.time() + self.capture_window
        raw_frames: Dict[int, List] = {}

        while time.time() < deadline and sum(len(v) for v in raw_frames.values()) < self.max_frames:
            try:
                rx = self.bus.recv(timeout=0.01)
            except Exception as e:
                logger.debug("recv error: %s", e)
                break
            if rx is None:
                continue
            if not (self.MIN_RSP_ID <= rx.arbitration_id <= self.MAX_RSP_ID):
                continue
            if rx.arbitration_id not in raw_frames:
                raw_frames[rx.arbitration_id] = []
            raw_frames[rx.arbitration_id].append(rx)

        messages: List[Message] = []
        for arb_id, frames in raw_frames.items():
            assembled = self._assemble_iso_tp(frames)
            if assembled:
                ecu_type = ECU.ENGINE if arb_id == self.ENGINE_RSP_ID else ECU.UNKNOWN
                messages.append(self._wrap_payload(assembled, ecu=ecu_type))
        return messages

    # -------------- ISO-TP reassembly (basic) --------------
    @staticmethod
    def _assemble_iso_tp(frames: List['can.Message']) -> Optional[bytes]:  # type: ignore
        # sort by timestamp to ensure order
        frames = sorted(frames, key=lambda m: m.timestamp)
        if not frames:
            return None
        first = frames[0]
        if len(first.data) == 0:
            return None
        pci = first.data[0]
        frame_type = pci & 0xF0
        if frame_type == 0x00:  # single frame
            length = pci & 0x0F
            return bytes(first.data[1:1 + length])
        elif frame_type == 0x10:  # first frame
            length = ((pci & 0x0F) << 8) + first.data[1]
            collected = bytearray(first.data[2:])
            expected_sn = 1
            for f in frames[1:]:
                if len(f.data) == 0:
                    continue
                pci2 = f.data[0]
                if (pci2 & 0xF0) != 0x20:
                    continue
                sn = pci2 & 0x0F
                # Accept wrap-around; simple check
                if sn != (expected_sn & 0x0F):
                    # out of sequence; abort
                    break
                collected.extend(f.data[1:])
                expected_sn += 1
                if len(collected) >= length:
                    break
            return bytes(collected[:length])
        else:
            # unsupported first frame type encountered alone
            return None

    @staticmethod
    def _wrap_payload(payload: bytes, ecu: int) -> Message:
        # Construct a Message compatible with downstream decoders
        m = Message([])
        m.ecu = ecu
        m.data = bytearray(payload)
        return m

    # Compatibility no-ops (OBD may call these for ELM)
    def low_power(self):  # pragma: no cover
        return None

    def normal_power(self):  # pragma: no cover
        return None
