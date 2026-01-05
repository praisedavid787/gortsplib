# CGNAT Port Learning for RTSP UDP Transport

## Problem Description

When mobile clients behind Carrier-Grade NAT (CGNAT) connect to an RTSP server, the NAT rewrites their source ports. The server receives packets from the actual CGNAT ports but sends RTCP Receiver Reports to the announced SETUP ports, which don't exist after the NAT rewrite.

### Flow of the Problem:

```
Mobile Client SETUP: client_port=5000-5001
        ↓
CGNAT rewrites to: 60000-60001
        ↓
Server receives from: 102.215.57.2:60000-60001
        ↓
Server sends RTCP to: 102.215.57.2:5001 ❌ (WRONG - port doesn't exist!)
        ↓
RTCP packets dropped by CGNAT
        ↓
Mobile client never receives RTCP Receiver Reports
        ↓
RTCP watchdog triggers false positives
        ↓
No way to detect real network failures
```

## Solution

Implement automatic CGNAT port learning by:
1. Extracting actual source ports from received RTP/RTCP packets
2. Updating server's write addresses to use learned ports
3. Sending RTCP to the correct CGNAT-mapped ports

### Flow of the Solution:

```
1. Server receives first RTP packet from actual port 60000
2. Server learns: "Real port is 60000, not 5000!"
3. Server updates udpRTPWriteAddr.Port = 60000
4. Server receives first RTCP packet from port 60001
5. Server updates udpRTCPWriteAddr.Port = 60001
6. Server now sends RTCP RR to correct port 60001 ✅
7. Mobile client receives RTCP RR successfully!
```

## Implementation Details

### 1. Updated Callback Signature

**File**: `server_session.go`

Changed the `readFunc` type to include source address:

```go
// readFunc is a callback called when a packet is received.
// data is the packet payload, addr is the source address (for CGNAT port learning).
type readFunc func(data []byte, addr *net.UDPAddr) bool
```

**Why**: The source address is needed to learn the actual CGNAT ports from received packets.

### 2. Added CGNAT Tracking Fields

**File**: `server_session_media.go`

Added tracking to `serverSessionMedia` struct:

```go
// CGNAT support: Track actual source ports from received packets
learnedSourceAddr    bool   // Whether we've learned the actual source address
actualSourceRTPPort  int    // Actual RTP port from received packets
actualSourceRTCPPort int    // Actual RTCP port from received packets
```

### 3. Port Learning Logic

**File**: `server_session_media.go`

#### RTP Port Learning (in `readPacketRTPUDPRecord`):

```go
// CGNAT port learning: Update write addresses on first packet
if !sm.learnedSourceAddr && addr != nil {
    sm.learnedSourceAddr = true
    sm.actualSourceRTPPort = addr.Port

    // Update RTP write address to use actual source port
    sm.udpRTPWriteAddr.Port = addr.Port

    // Assume RTCP port is RTP+1 (will be updated when we receive RTCP)
    sm.actualSourceRTCPPort = addr.Port + 1
    sm.udpRTCPWriteAddr.Port = addr.Port + 1

    log.Printf("[RTSP] Learned CGNAT RTP port: %d (announced: %d), updating write address",
        addr.Port, sm.udpRTPReadPort)
}
```

#### RTCP Port Learning (in `readPacketRTCPUDPRecord`):

```go
// CGNAT port learning: Update RTCP write address on first RTCP packet
if sm.learnedSourceAddr && addr != nil && addr.Port != sm.actualSourceRTCPPort {
    sm.actualSourceRTCPPort = addr.Port
    sm.udpRTCPWriteAddr.Port = addr.Port

    log.Printf("[RTSP] Learned CGNAT RTCP port: %d (assumed RTP+1: %d), updating write address",
        addr.Port, sm.actualSourceRTPPort+1)
}
```

### 4. Updated UDP Listener

**File**: `server_udp_listener.go`

Modified to pass source address to callbacks:

```go
// Pass source address for CGNAT port learning
for _, cb = range callbacks {
    if cb(buf[:n], addr) {
        createNewBuffer()
        break
    }
}
```

### 5. Client-Side Updates

All client read functions updated to match the new signature:

**Files modified**:
- `client_media.go` - All 8 read functions
- `client_udp_listener.go` - Pass source address
- `client_reader.go` - Pass nil for TCP
- `server_conn_reader.go` - Pass nil for TCP

**Example from `client_reader.go`**:
```go
if cb, ok := r.c.tcpCallbackByChannel[what.Channel]; ok {
    cb(what.Payload, nil) // No source address for TCP
}
```

## Files Modified

1. `server_session.go` - Updated `readFunc` type signature
2. `server_session_media.go` - Added CGNAT tracking and learning logic
3. `server_udp_listener.go` - Pass source address to callbacks
4. `client_media.go` - Updated all 8 read function signatures
5. `client_udp_listener.go` - Pass source address in client
6. `client_reader.go` - Pass nil for TCP callbacks
7. `server_conn_reader.go` - Pass nil for TCP callbacks

## Null Safety

All implementations include proper nil checks:

```go
if !sm.learnedSourceAddr && addr != nil {
    // Safe to use addr.Port
}
```

For TCP callbacks where there's no source address, we explicitly pass `nil` and document this behavior.

## Benefits

1. **RTCP Delivery**: RTCP Receiver Reports now reach mobile clients through CGNAT
2. **Accurate Monitoring**: RTCP watchdog can now detect real network failures
3. **Better Metrics**: Clients receive accurate packet loss, jitter, and RTT data
4. **Backward Compatible**: Works with both CGNAT and non-CGNAT scenarios
5. **Per-Session**: Each session learns its own ports, supporting multiple publishers from same IP

## Testing

Build succeeds with `go build .`

To test in a real CGNAT scenario:
1. Connect mobile client through CGNAT
2. Observe logs for port learning messages
3. Verify RTCP packets are received by mobile client
4. Confirm RTCP watchdog no longer triggers false positives

## Related Features

This fix works in conjunction with:
- Multi-publisher CGNAT support (IP wildcard routing)
- RTCP watchdog for mobile clients
- SSRC-based packet filtering for multi-publisher scenarios
