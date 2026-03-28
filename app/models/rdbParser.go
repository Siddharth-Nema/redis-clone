package models

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

// Constants for RDB opcodes
const (
	OpcodeMetadata = 0xFA
	OpcodeDBSelect = 0xFE
	OpcodeResizeDB = 0xFB
	OpcodeExpireMs = 0xFC
	OpcodeExpireS  = 0xFD
	OpcodeEOF      = 0xFF
)

type RDBParser struct {
	r *bufio.Reader
}

func NewRDBParser(r io.Reader) *RDBParser {
	return &RDBParser{r: bufio.NewReader(r)}
}

// Parse runs the main loop to process the RDB file sections
func (p *RDBParser) Parse() (map[string]string, error) {

	data := make(map[string]string)

	// 1. Header Section
	header := make([]byte, 9)
	if _, err := io.ReadFull(p.r, header); err != nil {
		return data, fmt.Errorf("failed to read header: %w", err)
	}
	fmt.Printf("Header: %s\n", string(header))

	// 2. Main Parsing Loop
	for {
		b, err := p.r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return data, err
		}

		switch b {
		case OpcodeMetadata:
			key, err := p.ReadString()
			if err != nil {
				return data, err
			}
			val, err := p.ReadString()
			if err != nil {
				return data, err
			}
			fmt.Printf("Metadata: %s = %s\n", key, val)

		case OpcodeDBSelect:
			dbIndex, _, err := p.ReadLength()
			if err != nil {
				return data, err
			}
			fmt.Printf("Database selected: %d\n", dbIndex)

		case OpcodeResizeDB:
			hashSize, _, err := p.ReadLength()
			if err != nil {
				return data, err
			}
			expireSize, _, err := p.ReadLength()
			if err != nil {
				return data, err
			}
			fmt.Printf("DB Resize: HashSize=%d, ExpireSize=%d\n", hashSize, expireSize)

		case OpcodeExpireMs:
			expireBytes := make([]byte, 8)
			if _, err := io.ReadFull(p.r, expireBytes); err != nil {
				return data, err
			}
			expireMs := binary.LittleEndian.Uint64(expireBytes)

			// Next byte is value type
			valueType, _ := p.r.ReadByte()
			key, _ := p.ReadString()
			val, _ := p.ReadString()
			fmt.Printf("Key-Value (Expire %d ms): %s = %s (Type: %d)\n", expireMs, key, val, valueType)

		case OpcodeExpireS:
			expireBytes := make([]byte, 4)
			if _, err := io.ReadFull(p.r, expireBytes); err != nil {
				return data, err
			}
			expireS := binary.LittleEndian.Uint32(expireBytes)

			// Next byte is value type
			valueType, _ := p.r.ReadByte()
			key, _ := p.ReadString()
			val, _ := p.ReadString()
			fmt.Printf("Key-Value (Expire %d s): %s = %s (Type: %d)\n", expireS, key, val, valueType)

		case OpcodeEOF:
			// Read 8-byte CRC64 checksum
			checksum := make([]byte, 8)
			io.ReadFull(p.r, checksum)
			fmt.Printf("EOF reached. Checksum: %x\n", checksum)
			return data, nil

		default:
			// If it's none of the opcodes above, it's a standard Key-Value pair without an expire.
			// The byte we just read IS the value type (usually 0x00 for string).
			valueType := b
			key, err := p.ReadString()
			if err != nil {
				return data, err
			}
			val, err := p.ReadString()
			if err != nil {
				return data, err
			}
			data[key] = val
			fmt.Printf("Key-Value: %s = %s (Type: %d)\n", key, val, valueType)
		}
	}
	return data, nil
}

// ReadLength parses length-encoded integers based on the first two bits
func (p *RDBParser) ReadLength() (uint32, bool, error) {
	b, err := p.r.ReadByte()
	if err != nil {
		return 0, false, err
	}

	flag := b >> 6 // Get the first two bits

	switch flag {
	case 0: // 00: The length is the remaining 6 bits
		return uint32(b & 0x3F), false, nil
	case 1: // 01: The length is the next 14 bits (big-endian)
		nextByte, err := p.r.ReadByte()
		if err != nil {
			return 0, false, err
		}
		length := (uint32(b&0x3F) << 8) | uint32(nextByte)
		return length, false, nil
	case 2: // 10: Discard 6 bits, next 4 bytes are length (big-endian)
		lengthBytes := make([]byte, 4)
		if _, err := io.ReadFull(p.r, lengthBytes); err != nil {
			return 0, false, err
		}
		length := binary.BigEndian.Uint32(lengthBytes)
		return length, false, nil
	case 3: // 11: Special string encoding format
		return uint32(b & 0x3F), true, nil
	}

	return 0, false, fmt.Errorf("invalid length encoding format")
}

// ReadString handles both raw byte strings and special integer strings
func (p *RDBParser) ReadString() (string, error) {
	length, isSpecial, err := p.ReadLength()
	if err != nil {
		return "", err
	}

	if isSpecial {
		switch length {
		case 0: // 0xC0: 8-bit integer
			b, err := p.r.ReadByte()
			if err != nil {
				return "", err
			}
			return strconv.Itoa(int(int8(b))), nil
		case 1: // 0xC1: 16-bit integer (little-endian)
			bytes := make([]byte, 2)
			if _, err := io.ReadFull(p.r, bytes); err != nil {
				return "", err
			}
			return strconv.Itoa(int(int16(binary.LittleEndian.Uint16(bytes)))), nil
		case 2: // 0xC2: 32-bit integer (little-endian)
			bytes := make([]byte, 4)
			if _, err := io.ReadFull(p.r, bytes); err != nil {
				return "", err
			}
			return strconv.Itoa(int(int32(binary.LittleEndian.Uint32(bytes)))), nil
		default:
			return "", fmt.Errorf("unsupported special string encoding type: %d", length)
		}
	}

	// Normal string: read 'length' bytes
	strBytes := make([]byte, length)
	if _, err := io.ReadFull(p.r, strBytes); err != nil {
		return "", err
	}
	return string(strBytes), nil
}
