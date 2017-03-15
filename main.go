package main

import "fmt"
import "os"
import "strings"
import "github.com/cactus/go-statsd-client/statsd"
import "gopkg.in/natefinch/lumberjack.v2"
import "time"
import "gopkg.in/urfave/cli.v1"
import "github.com/op/go-logging"
import "github.com/hpcloud/tail"


var builddate string

var app *cli.App = cli.NewApp()
var log = logging.MustGetLogger("logs2kafka")

func init() {
    app.EnableBashCompletion = true
    app.Version = builddate
}

func main() {
    app.Name = "logs2kafka"
    app.Usage = "This application receivers logs via syslog/udp and forwards them to a Kafka cluster. It will also store local copies to FILE_LOGS_PATH (with log rotation) and allow to tail them locally. The logs are converted (if they aren't already) into a JSON format so that each log entry is its own JSON document. Each document will have at least 'ts' attribute with an ISO8601 timestamp."

    app.Flags = []cli.Flag{
        cli.BoolFlag{
            Name:   "debug",
            Usage:  "Print out more debug information to stderr",
            EnvVar: "LOGS2KAFKA_DEBUG",
        },
        cli.BoolFlag{
            Name:   "disable-kafka",
            Usage:  "Disable kafka producing",
        },        
        cli.StringFlag{
            Name:   "default-topic",
            Value:  "unknown",
            Usage:  "Default if 'service' attribute can't be used.",
            EnvVar: "TOPIC",
        },
        cli.StringFlag{
            Name:   "topic-prefix",
            Value:  "service",
            Usage:  "The kafka topic will be '<topic-prefix>.<service name>'",
            EnvVar: "TOPIC_PREFIX",
        },        
        cli.StringFlag{
            Name:   "server-ip",
            Usage:  "The ip of this server, which will be placed into 'server_ip' attribute if present.",
            EnvVar: "SERVER_IP",
        },
        cli.StringFlag{
            Name:   "kafka-connection-string",
            Usage:  "Comma delimited list of host:port pairs for the broker where to connect.",
            Value: "localhost:9092",
            EnvVar: "KAFKA_CONNECTION_STRING",
        },
        cli.IntFlag{
            Name:   "syslog-port",
            Usage:  "Port where to listen syslog messages in UDP",
            Value: 8601,
            EnvVar: "SYSLOG_LISTEN_PORT",
        },
        cli.StringFlag{
            Name:   "statsd-host",
            Usage:  "Host where to send statsd metrics.",
            Value: "localhost",
            EnvVar: "STATSD_HOST",
        },
        cli.IntFlag{
            Name:   "statsd-port",
            Usage:  "Port (in the statsd-host) where to send statsd metrics.",
            Value: 8125,
            EnvVar: "STATSD_HOST",
        },
        cli.StringFlag{
            Name:   "file-logs-path",
            Usage:  "Directory where to store local copies of the log files.",
            Value: "/tmp",
            EnvVar: "LOGS2KAFKA_FILE_LOGS_PATH",
        },
    }

    app.Commands = []cli.Command{
        {
            Name: "tail",
            Usage: "Tail logs of a local service instead of connecting to a centralised Kafka broker. Uses the local copies of the log files in the file-logs-path.",
            ArgsUsage: "service_name",
            Flags: []cli.Flag {
                cli.IntFlag{
                    Name:   "seek",
                    Usage:  "Seek n bytes from the end. Notice that this is in bytes, not lines. Printing will start from next new line after seek.",
                    Value: 20000,
                    EnvVar: "SEEK_BYTES",
                },
                cli.BoolFlag{
                    Name:   "raw",
                    Usage:  "Display raw JSON",
                },
                cli.BoolFlag{
                    Name:   "nofollow",
                    Usage:  "Just print last lines and don't try to follow new messages.",
                },                         
            },
            Action: func(c *cli.Context) error {
                if len(c.Args()) == 0 {
                    cli.ShowSubcommandHelp(c)
                    return cli.NewExitError("Please provide service name as first parameter", 1)
                }

                service := c.Args()[0]

                filenames := []string{c.GlobalString("file-logs-path") + "/" + service + ".log", c.GlobalString("file-logs-path") + "/service." + service + ".log"}
                var filename string = ""
                var seek int64 = int64(c.Int("seek"))
                for _, f := range filenames {
                    file, err := os.Open(f)
                    if err != nil {
                        if os.IsNotExist(err) {
                            if c.GlobalBool("debug") {
                                fmt.Fprintf(c.App.Writer, "File %s does not exists, trying next in search path\n", f)
                            }
                            continue
                        }
                        log.Fatal("Error: %+v\n", err)
                    }
                    fi, err := file.Stat()
                    if err != nil {
                        log.Fatal(err)
                    }
                    
                    
                    file_size := fi.Size()
                    if file_size < seek {
                        seek = file_size
                    }
                    file.Close()

                    filename = f
                }

                if filename == "" {
                    return cli.NewExitError("Operating in local file mode where logs2kafka can help you to view logs generated by containers in the same machine where you are using it.\nIn this mode you can't view centralised Kafka log feed.\nCould not find any log file anywhere. Please check that you have file-logs-path set correctly which should point to the local directory where copies of the logs are stored.\n", 1)
                } 

                config := tail.Config{}
                if !c.Bool("nofollow") {
                    config.Follow = true
                    config.ReOpen = true
                }
                config.Location = &tail.SeekInfo{-seek, os.SEEK_END}


                t, err := tail.TailFile(filename, config)
                if err != nil {
                    return cli.NewExitError(fmt.Sprintf("Could not start tailing file: %+v", err), 2)
                }
                fmt.Fprintf(cli.ErrWriter, "Tailing service %s logs file %s (seeking to %d from end)\n", service, filename, -seek)

                skip := false

                // If we are seeking we should skip the first line as the seek will most likelly not seek into a beginning of a line
                if seek != 0 {
                    skip = true
                }
                for line := range t.Lines {
                    if skip {
                        skip = false
                        continue
                    }

                    msg := JSONToMessage(line.Text)

                    if c.Bool("raw") {
                        fmt.Printf("%s\n", msg.Data)
                    } else {
                        ts, err1 := msg.GetString("ts")
                        level, err2 := msg.GetString("level")
                        if err2 != nil {
                            level = "UNKNOWN"
                        }
                        msg, err3 := msg.GetString("msg")
                        msg = strings.TrimRight(msg, "\n")

                        if err1 != nil || err3 != nil {
                            fmt.Printf("Raw message: %s\n", line.Text)
                        } else {
                            fmt.Printf("%s %s %s\n", ts, level, msg)                        
                        }
                    }
                }
                return nil
            },
        },
        {
            Name: "logs2kafka",
            Usage: "Start logs2kafka daemon mode: listen for messages and forward them to Kafka.",
            Action: func(c *cli.Context) error {
                fmt.Printf("Default action")

                kafka := KafkaProducer{}

                loggers := make(map[string]*lumberjack.Logger)

                default_topic := c.GlobalString("default-topic")
                topic_prefix := c.GlobalString("topic-prefix")
                brokers := strings.Split(c.GlobalString("kafka-connection-string"), ",")
                syslog_port := c.GlobalInt("syslog-port")
                statsd_host := c.GlobalString("statsd-host")
                statsd_port := c.GlobalInt("statsd-port")
                file_logs_path := c.GlobalString("file-logs-path")
                server_ip := c.GlobalString("server-ip")

                fmt.Fprintf(os.Stderr, "Starting logs2kafka (build %s) with the following settings\n", builddate)
                fmt.Fprintf(os.Stderr, "default-topic: %s\n", default_topic)
                fmt.Fprintf(os.Stderr, "topic_prefix: %s\n", topic_prefix)
                fmt.Fprintf(os.Stderr, "brokers: %+v\n", brokers)
                fmt.Fprintf(os.Stderr, "syslog listen port: %d\n", syslog_port)
                fmt.Fprintf(os.Stderr, "statsd host: %s\n", statsd_host)
                fmt.Fprintf(os.Stderr, "statsd port: %d\n", statsd_port)
                fmt.Fprintf(os.Stderr, "server ip: %s\n", server_ip)
                fmt.Fprintf(os.Stderr, "directory where to log local copies: %s\n", file_logs_path)

                hostname, err := os.Hostname()
                if err != nil {
                    panic(err)
                }

                if !c.GlobalBool("disable-kafka") {
                    err = kafka.Init(brokers, hostname)
                    if err != nil {
                        fmt.Fprintf(os.Stderr, "Error getting opening kafka connection: %+v\n", err)
                    }
                }

                statsd, err := statsd.NewClient(fmt.Sprintf("%s:%d", statsd_host, statsd_port), "")
                if err != nil {
                    fmt.Fprintf(os.Stderr, "Error opening statsd connection: %+v\n", err)
                }
                statsd.Inc("logs2kafka.app.started", 1, 1)

                syslog := Syslog{}
                syslog.Messages = make(chan Message)
                syslog.Init(int(syslog_port))


                serverInfo := ServerInfo{}
                serverInfo.ServerIP = server_ip
                serverInfo.Hostname = hostname

                go func(c chan Message) {
                    m := JSONToMessage("{}")
                    m.ParseJSON()
                    m.Container.Set(fmt.Sprintf("logs2kafka starting at %s\n", time.Now().UTC().Format(time.RFC3339Nano)), "msg")
                    m.Container.Set("logs2kafka", "service")
                    m.Container.Set("INFO", "level")
                    c <- m
                }(syslog.Messages)

                for message := range syslog.Messages {
                    if c.GlobalBool("debug") {
                        fmt.Printf("Got message: %+v\n", message)     
                    }
                    EnsureMessageFormat(serverInfo, &message)
                    SendStatsdMetricsFromMessage(statsd, &message)

                    if message.Topic == "" {
                        message.Topic = default_topic
                    }
                    message.Topic = topic_prefix + "." + message.Topic

                    if loggers != nil {

                        filename := file_logs_path + "/" + message.Topic + ".log"
                        if c.GlobalBool("debug") {
                            fmt.Printf("Going to write message to file %s\n", filename)     
                        }

                        var logger *lumberjack.Logger = nil


                        logger, ok := loggers[message.Topic]
                        if !ok {
                            if c.GlobalBool("debug") {
                                fmt.Printf("Creating new logger for topic %s\n", message.Topic)
                            }

                            logger = &lumberjack.Logger{
                                Filename:   filename,
                                MaxSize:    100, // megabytes
                                MaxBackups: 3,
                            }

                            loggers[message.Topic] = logger

                        }
                        logger.Write([]byte(message.Container.String() + "\n"))
                    }

                    if !c.GlobalBool("disable-kafka") {
                        kafka.Produce(message)
                    }

                }

                return nil
            },
        },
    }

    app.Run(os.Args)
}

