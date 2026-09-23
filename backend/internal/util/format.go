// Package util contains small, dependency-free helpers shared across the
// backend. FormatBytes and FormatBitrate are the single source of truth for
// human-readable byte/bitrate formatting - Telegram notifications, HTTP
// responses, and subscription placeholder rendering all call into this
// package instead of keeping their own copies.
package util

import (
	"math/big"
	"strconv"
)

var byteUnits = [...]string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

// FormatBytes converts a byte count into a human-readable IEC string
// (e.g. "372.53 GiB"), auto-scaling the unit up to EiB. Negative values are
// formatted with a leading "-" against their absolute value.
func FormatBytes(bytes int64) string {
	if bytes == 0 {
		return "0 B"
	}

	negative := false
	value := float64(bytes)
	if value < 0 {
		negative = true
		value = -value
	}

	const step = 1024.0
	unit := 0
	for value >= step && unit < len(byteUnits)-1 {
		value /= step
		unit++
	}

	var buf [32]byte
	b := buf[:0]
	if negative {
		b = append(b, '-')
	}
	if unit == 0 {
		b = strconv.AppendInt(b, int64(value), 10)
	} else {
		b = strconv.AppendFloat(b, value, 'f', 2, 64)
	}
	b = append(b, ' ')
	b = append(b, byteUnits[unit]...)
	return string(b)
}

// FormatBigBytes is the *big.Int counterpart of FormatBytes, for aggregate
// values (e.g. panel-wide traffic totals) that may exceed the int64 range.
func FormatBigBytes(value *big.Int) string {
	if value == nil || value.Sign() == 0 {
		return "0 B"
	}

	negative := false
	abs := new(big.Int).Set(value)
	if abs.Sign() < 0 {
		negative = true
		abs.Abs(abs)
	}

	floatValue, _ := new(big.Float).SetInt(abs).Float64()

	const step = 1024.0
	unit := 0
	for floatValue >= step && unit < len(byteUnits)-1 {
		floatValue /= step
		unit++
	}

	var buf [32]byte
	b := buf[:0]
	if negative {
		b = append(b, '-')
	}
	b = strconv.AppendFloat(b, floatValue, 'f', 2, 64)
	b = append(b, ' ')
	b = append(b, byteUnits[unit]...)
	return string(b)
}

var bitrateUnits = [...]string{"bps", "Kbps", "Mbps", "Gbps", "Tbps"}

// FormatBitrate converts a bits-per-second value into a human-readable
// string (e.g. "1.35 Gbps"), auto-scaling the unit up to Tbps. Negative
// values are formatted with a leading "-" against their absolute value.
func FormatBitrate(bitsPerSecond float64) string {
	if bitsPerSecond == 0 {
		return "0 bps"
	}

	negative := false
	value := bitsPerSecond
	if value < 0 {
		negative = true
		value = -value
	}

	const step = 1000.0
	unit := 0
	for value >= step && unit < len(bitrateUnits)-1 {
		value /= step
		unit++
	}

	var buf [32]byte
	b := buf[:0]
	if negative {
		b = append(b, '-')
	}
	if unit == 0 {
		b = strconv.AppendFloat(b, value, 'f', 0, 64)
	} else {
		b = strconv.AppendFloat(b, value, 'f', 2, 64)
	}
	b = append(b, ' ')
	b = append(b, bitrateUnits[unit]...)
	return string(b)
}
