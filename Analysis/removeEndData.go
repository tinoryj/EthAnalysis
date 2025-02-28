package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func processLogFile(filePath, outputPath string) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(fmt.Sprintf("Failed to open file: %s", filePath))
	}
	defer file.Close()
	outputFile, err := os.Create(outputPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to create output file: %s", outputPath))
	}
	defer outputFile.Close()

	reader := bufio.NewReader(file)
	lineCount := 0
	for {
		line, err := reader.ReadString('\n') // Read until newline
		if err != nil {
			if err == io.EOF {
				fmt.Println("End of file reached")
				break
			}
			fmt.Println("Error reading file:", err)
			return
		}
		lineCount++
		if strings.Contains(line, "Processing block (end), ID: 21385047") {
			break
		}else {
			outputFile.WriteString(line)
		}
	}
	fmt.Printf("\rProcessed a total of %d lines.\n", lineCount)
}


func signalHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: program <log_file_path> <out_put_log_path>")
		return
	}
	logFilePath := os.Args[1]
	outPutLogPath := os.Args[2]
	signalHandler()
	processLogFile(logFilePath, outPutLogPath)
}
