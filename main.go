package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

//
// read command line parameters
//
func getConfig() (rate int, inflight int, command []string) {
	rate = 1
	inflight = 1

	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--rate":
			rate, _ = strconv.Atoi(os.Args[i+1])
			i++
		case "--inflight":
			inflight, _ = strconv.Atoi(os.Args[i+1])
			i++
		default:
			command = os.Args[i:]
			return
		}
	}

	return
}

//
// prepares a command.
// splits command and arguments.
// for each argument replaces {} to stdin line
// returns command and it agruments
//
func prepareCommand(command []string, parameter string) (cmd string, args []string) {
	cmd = command[0]
	args = append(args, command[1:]...)
	for i := range args {
		args[i] = strings.Replace(args[i], "{}", parameter, -1)
	}
	return
}

//
// runs a command
//
func runCommand(command string, arguments ...string) {
	cmd := exec.Command(command, arguments...)
	cmd.Stdout = os.Stdout
	cmd.Run()
}

//
// reads stdin lines and send them to output channel
//
func stdinReader() <-chan string {
	output := make(chan string)

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			output <- scanner.Text()
		}
		close(output)
	}()

	return output
}

//
// limits rate for incoming data and send data to output channel
//
func limiter(rate int, input <-chan string) <-chan string {
	output := make(chan string)

	go func() {
		freq := time.Second / time.Duration(rate)
		ticker := time.NewTicker(freq)
		first := true
		for in := range input {
			if !first {
				<-ticker.C
			} else {
				first = !first
			}
			output <- in
		}
		ticker.Stop()
		close(output)
	}()

	return output
}

//
// reads incoming data, forms a command and run it
//
func job(wg *sync.WaitGroup, command []string, input <-chan string) {
	defer wg.Done()

	for in := range input {
		cmd, agrs := prepareCommand(command, in)
		runCommand(cmd, agrs...)
	}
}

func main() {
	startAt := time.Now()

	rate, inflight, command := getConfig()
	input := limiter(rate, stdinReader())

	var wg sync.WaitGroup
	for i := 0; i < inflight; i++ {
		wg.Add(1)
		go job(&wg, command, input)
	}
	wg.Wait()

	fmt.Printf("%v elapsed\n", time.Since(startAt))
}
