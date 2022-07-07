package bitcask

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"github.com/google/uuid"
	"github.com/sony/sonyflake"
	"net"
	"os"
	"time"
)

// Lower16bitIpMacPid  机器的唯一id
func Lower16bitIpMacPid() (uint16, error) {
	var (
		mac string
		ip  string
	)
	netInterfaces, err := net.Interfaces()
	if err == nil {
		for _, netInterface := range netInterfaces {
			macAddr := netInterface.HardwareAddr.String()
			if len(macAddr) == 0 {
				continue
			}
			mac += macAddr
		}
	} else {
		mac = ""
	}

	interfaceAddr, err := net.InterfaceAddrs()
	if err == nil {
		for _, address := range interfaceAddr {
			ipNet, isValidIpNet := address.(*net.IPNet)
			if isValidIpNet && !ipNet.IP.IsLoopback() {
				if ipNet.IP.To4() != nil {
					ip += ipNet.IP.String()
				}
			}
		}
	} else {
		ip = ""
	}
	buffer := bytes.NewBuffer([]byte{})
	buffer.Write([]byte(mac))
	buffer.Write([]byte(ip))
	binary.Write(buffer, binary.BigEndian, os.Getpid())
	hash := sha256.New()

	id := uint16(uint64(binary.BigEndian.Uint64(hash.Sum(buffer.Bytes()))) ^ 0xffff)
	return id, nil

}

var snowflake = sonyflake.NewSonyflake(
	sonyflake.Settings{
		StartTime: time.Now(),
		MachineID: Lower16bitIpMacPid,
	})

// SnowflakeID  全局的分布式id生成器
func SnowflakeID() uint64 {
	id, err := snowflake.NextID()
	if err != nil {
		x := uuid.New()
		x.NodeID()
		binary.BigEndian.PutUint64(x[:], id)
		return id
	}
	return id
}
