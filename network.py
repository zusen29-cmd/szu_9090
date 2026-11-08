import asyncio
from utils import HELLXlog
async def HELLXconnect_online(ip, port, auth_hex):
    try:
        reader, writer = await asyncio.open_connection(ip, port)
        auth_packet = bytes.fromhex(auth_hex)
        writer.write(auth_packet)
        await writer.drain()
        HELLXlog("NETWORK", f"Online Connection Established ({ip}:{port})")
        return reader, writer
    except Exception as e:
        HELLXlog("NETWORK", f"Connection Error: {e}")
        return None, None
async def HELLXping_loop(writer, build_ping_packet, key, iv, interval=5.0):
    _ONLINE_PING = bytes.fromhex('0201')
    try:
        while writer and not writer.transport.is_closing():
            try:
                writer.write(_ONLINE_PING)
                await writer.drain()
            except Exception:
                pass
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
async def HELLXread_loop(reader, callback, key, iv):
    HELLXlog("NETWORK", "Reader Loop Started.")
    try:
        while True:
            data = await reader.read(16384)
            if not data:
                HELLXlog("NETWORK", "Connection Closed by server.")
                break
            await callback(data)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        HELLXlog("NETWORK", f"Reader Loop Exception: {e}")
    finally:
        HELLXlog("NETWORK", "Reader Loop Stopped.")
