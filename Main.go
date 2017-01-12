package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"path"
)

const versionString = "0.1.0"

func main() {
	handleOsSignals()

	var commandArgs []string
	var mainCommand string
	if len(os.Args) >= 2 {
		mainCommand = os.Args[1]
		commandArgs = os.Args[2:]
	} else {
		mainCommand = ""
	}

	switch mainCommand {
	case "start":
		parseStartCommand(commandArgs)
	case "generate":
		parseGenerateCommand(commandArgs)
	case "version":
		fmt.Println(versionString)
	default:
		fmt.Println("Supported commands:")
		fmt.Println("")
		fmt.Println("  zincserver start")
		fmt.Println("  \tStart a new server instance.")
		fmt.Println("  zincserver generate")
		fmt.Println("  \tGenerate a test datastore (for testing purposes).")
		fmt.Println("  zincserver version")
		fmt.Println("  \tPrint version info.")
	}
}

func parseStartCommand(args []string) {
	commandOptions := DefaultServerStartupOptions()
	helpRequested := false

	commandFlagSet := flag.NewFlagSet("start", flag.PanicOnError)

	commandFlagSet.Int64Var(&commandOptions.InsecurePort, "insecurePort", commandOptions.InsecurePort, "Port to use for insecure connections.")
	commandFlagSet.Int64Var(&commandOptions.SecurePort, "securePort", commandOptions.SecurePort, "Port to use for secure connections. Requires valid 'certFile' and 'keyFile' arguments to be provided as well.")
	commandFlagSet.BoolVar(&commandOptions.EnableHTTP2, "enableHTTP2", commandOptions.EnableHTTP2, "Enable HTTP2 support. Only relevant when secure connections are enabled.")
	commandFlagSet.StringVar(&commandOptions.CertFile, "certFile", commandOptions.CertFile, "Path to a certificate file (X.509) to use with secure connections.")
	commandFlagSet.StringVar(&commandOptions.KeyFile, "keyFile", commandOptions.KeyFile, "Path to a private key file (X.509) to use with secure connections.")
	commandFlagSet.StringVar(&commandOptions.StoragePath, "storagePath", commandOptions.StoragePath, "Root datastore storage directory path. *Note*: the storage directory should not contain any files other than ZincServer data files! (required)")
	commandFlagSet.Int64Var(&commandOptions.LogLevel, "logLevel", commandOptions.LogLevel, "Logging level.")
	commandFlagSet.BoolVar(&commandOptions.NoAutoMasterKey, "noAutoMasterKey", commandOptions.NoAutoMasterKey, "Suppress generation of a random master key when a default configuration is created. Leave it empty instead (highly insecure, should only be used for testing).")
	commandFlagSet.BoolVar(&helpRequested, "help", helpRequested, "Show this help message.")
	commandFlagSet.Parse(args)

	printHelp := func() {
		fmt.Println("Supported arguments:")
		fmt.Println("")
		commandFlagSet.PrintDefaults()
	}

	if helpRequested == true {
		printHelp()
		return
	}

	if commandOptions.StoragePath == "" {
		fmt.Println("")
		fmt.Println("Error: no storage path specified. Please use '-storagePath <directory path>' to specify the root datastore storage directry.")
		fmt.Println("")
		printHelp()
		return
	}

	// Normalize storage path to remove unnecessary slashes and dots
	commandOptions.StoragePath = path.Clean(commandOptions.StoragePath)

	if commandOptions.InsecurePort == 0 && commandOptions.SecurePort == 0 {
		fmt.Println("")
		fmt.Println("Error: no port specified. At least one of 'insecurePort' or 'securePort' arguments must be provided.")
		fmt.Println("")

		printHelp()
		return
	}

	fmt.Println("ZincServer v" + versionString)
	fmt.Println("")
	fmt.Println(`---------------------------------------------------------
| Warning:                                              |
|   This is alpha software. Use at your own discretion! |
|   There might be bugs or security vulnerabilities.    |
|                                                       |
|   Please report issues at:                            |
|   https://github.com/zincbase/zincserver/issues       |
---------------------------------------------------------`)

	server := NewServer(commandOptions)
	server.Start()
	server.runningStateWaitGroup.Wait()
}

func parseGenerateCommand(args []string) {
	path := ""
	entryCount := 100
	keySize := 20
	valueSize := 100
	entryType := "randomPathEntry"
	showHelp := false

	commandFlagSet := flag.NewFlagSet("generate", flag.PanicOnError)

	commandFlagSet.StringVar(&path, "path", path, "Path of datastore file to generate. (required)")
	commandFlagSet.IntVar(&entryCount, "entryCount", entryCount, "Number of entries to generate.")
	commandFlagSet.IntVar(&keySize, "keySize", keySize, "Key size.")
	commandFlagSet.IntVar(&valueSize, "valueSize", valueSize, "Value size.")
	commandFlagSet.StringVar(&entryType, "entryType", entryType, `Entry type. Can be either one of "randomPathEntry", "randomPathEntryWithBinaryValue", "randomUTF8Entry", "randomBinaryEntry", "randomAlphanumericEntry" or "randomJSONEntry".`)	
	commandFlagSet.BoolVar(&showHelp, "help", showHelp, "Show this help message.")
	commandFlagSet.Parse(args)

	if (showHelp) {
		commandFlagSet.PrintDefaults()
	} else if path == "" {
		fmt.Println("")
		fmt.Println("Error: no target path specified. Please specify a path for the generated file using '-path <targetFilePath>'.")
		fmt.Println("")
		commandFlagSet.PrintDefaults()
	} else {
		GenerateRandomDatastore(path, entryCount, keySize, valueSize, entryType)
	}
}

func handleOsSignals() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		//fmt.Println("SIGTERM")
		os.Exit(1)
	}()
}
