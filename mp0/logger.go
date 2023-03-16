package main

// This is the logger program for the MP0, for reference, we used some of the
//code from the following official open sources:
//https://pkg.go.dev/strconv, https://pkg.go.dev/strconv, https://pkg.go.dev/time#Ticker, https://pkg.go.dev/os#OpenFile
import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	arguments := os.Args
	fmt.Print("starting event logger...\n")
	os.Truncate("bandwidth.txt", 0)
	os.Truncate("time_logger.txt", 0)
	c, err := net.Listen("tcp", arguments[1]+":"+arguments[2])
	if err != nil {
		fmt.Println("The server is not working")
		return
	}
	defer c.Close()
	delay := make(chan float64)
	bandwidth := make(chan int)
	go file_writer(delay, bandwidth)
	for {
		conn, err := c.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go connectionHandler(conn, delay, bandwidth)
	}
}

func connectionHandler(conn net.Conn, delay chan float64, bandwidth chan int) {
	defer conn.Close()

	Reader := bufio.NewReader(conn)
	node_num, _, _ := Reader.ReadLine()
	node := string(node_num)
	fmt.Printf("%.7f - %s connected\n", get_sys_time(), node)
	for {
		message, _, _ := Reader.ReadLine()
		someshit := strings.Split(string(message), " ")
		if someshit[0] != "0" && len(someshit) == 2 {
			byte_num := len(message)
			fmt.Printf("%.7f %s %s\n", get_sys_time(), node, someshit[1])
			float_generated, _ := strconv.ParseFloat(someshit[0], 64)
			delay <- (get_sys_time() - float_generated)
			bandwidth <- byte_num
		} else {
			fmt.Printf("%f - %s disconnected\n", get_sys_time(), node)
			return
		}
	}
}

func write_file(S string) {
	f, err := os.OpenFile("time_logger.txt", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Print("writing to file failed")
	}
	defer f.Close()

	if _, err = f.WriteString(S + "\n"); err != nil {
		panic(err)
	}
}
func write_bandwidth(S string) {
	f, err := os.OpenFile("bandwidth.txt", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Print("writing to file failed")
	}
	defer f.Close()

	if _, err = f.WriteString(S + "\n"); err != nil {
		panic(err)
	}
}

func min(time_slots []float64) string {
	if len(time_slots) == 0 {
		return "0"
	}
	min := time_slots[0]
	for _, value := range time_slots {
		if value < min {
			min = value
		}
	}
	return strconv.FormatFloat(min, 'f', 7, 64)
}
func max(time_slots []float64) string {
	if len(time_slots) == 0 {
		return "0"
	}
	max := time_slots[0]
	for _, value := range time_slots {
		if value > max {
			max = value
		}
	}
	return strconv.FormatFloat(max, 'f', 7, 64)
}
func avarage(time_slots []float64) string {
	var sum float64
	if len(time_slots) == 0 {
		return "0"
	}
	for _, value := range time_slots {
		sum += value
	}
	return strconv.FormatFloat(sum/float64(len(time_slots)), 'f', 7, 64)
}

func ninety_percentile(time_slots []float64) string {
	if len(time_slots) == 0 {
		return "0"
	}
	sort.Float64s(time_slots)
	return strconv.FormatFloat(time_slots[(len(time_slots)*9)/10], 'f', 7, 64)
}

func get_sys_time() float64 {
	return (float64)(float64(time.Now().UnixMicro()) / float64(1000000))
}

func file_writer(delay chan float64, bandwidth chan int) {
	timer := time.NewTicker(time.Second)
	var timeslots []float64
	byte_sec := 0
	defer timer.Stop()
	for {
		select {
		case delay_ss := <-delay:
			timeslots = append(timeslots, delay_ss)
		case bandwidth_ := <-bandwidth:
			byte_sec += bandwidth_
		case <-timer.C:
			delay_ := min(timeslots) + " " + max(timeslots) + " " + avarage(timeslots) + " " + ninety_percentile(timeslots)
			if byte_sec == 0 {
				break
			}
			write_file(delay_)
			write_bandwidth(strconv.Itoa(byte_sec * 8))
			byte_sec = 0
			timeslots = nil
		default:
		}
	}
}
