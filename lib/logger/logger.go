package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// Settings stores config for Logger
type Settings struct {
	Path       string `yaml:"path"`
	Name       string `yaml:"name"`
	Ext        string `yaml:"ext"`
	TimeFormat string `yaml:"time-format"`
}

type logLevel int

// Output levels
const (
	DEBUG logLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

const (
	flags              = log.LstdFlags // 表示输出的日志包括标准的日期和时间
	defaultCallerDepth = 2
	bufferSize         = 1e5
)

var (
	levelFlags = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
)

type logEntry struct {
	msg   string
	level logLevel
}

// Logger is Logger
type Logger struct {
	logFile   *os.File       // 指向打开的日志文件
	logger    *log.Logger    // Go标准库中的log
	entryChan chan *logEntry // 用于异步地将日志传递给后台处理
	entryPool *sync.Pool     // 重用日志记录，在Conn中也用到了这个方法
}

/* ---- New Logger ---- */

func NewStdoutLogger() *Logger {
	logger := &Logger{
		logFile:   nil,
		logger:    log.New(os.Stdout, "", flags),
		entryChan: make(chan *logEntry, bufferSize),
		entryPool: &sync.Pool{
			New: func() any {
				return &logEntry{}
			},
		},
	}
	go func() {
		for e := range logger.entryChan {
			// calldepth用于控制回溯多少层调用栈，以确定日志发生的位置
			_ = logger.logger.Output(0, e.msg) // msg includes call stack, no need for calldepth
			logger.entryPool.Put(e)
		}
	}()
	return logger
}

// NewFileLogger creates a logger which print msg to stdout and log file
func NewFileLogger(settings *Settings) (*Logger, error) {
	fileName := fmt.Sprintf("%s-%s.%s",
		settings.Name,
		time.Now().Format(settings.TimeFormat),
		settings.Ext)
	logFile, err := mustOpen(fileName, settings.Path)
	if err != nil {
		return nil, fmt.Errorf("logging.Join err: %s", err)
	}
	// MultiWriter creates a writer that duplicates its writes to all the provided writers
	mw := io.MultiWriter(os.Stdout, logFile)
	logger := &Logger{
		logFile:   logFile,
		logger:    log.New(mw, "", flags),
		entryChan: make(chan *logEntry, bufferSize),
		entryPool: &sync.Pool{
			New: func() any {
				return &logEntry{}
			},
		},
	}
	go func() {
		for e := range logger.entryChan {
			logFilename := fmt.Sprintf("%s-%s.%s",
				settings.Name,
				time.Now().Format(settings.TimeFormat),
				settings.Ext)
			// 判断文件名是否一致
			if path.Join(settings.Path, logFilename) != logger.logFile.Name() {
				logFile, err := mustOpen(logFilename, settings.Path)
				if err != nil {
					panic(any("open log " + logFilename + " failed: " + err.Error()))
				}
				logger.logFile = logFile
				logger.logger = log.New(io.MultiWriter(os.Stdout, logFile), "", flags)
			}
			_ = logger.logger.Output(0, e.msg) // msg includes call stack, no need for calldepth
			logger.entryPool.Put(e)
		}
	}()
	return logger, nil
}

var DefaultLogger = NewStdoutLogger()

// Setup initializes DefaultLogger
func Setup(settings *Settings) {
	logger, err := NewFileLogger(settings)
	if err != nil {
		panic(any(err))
	}
	DefaultLogger = logger
}

// Output sends a msg to logger
func (logger *Logger) Output(level logLevel, callerDepth int, msg string) {
	var formattedMsg string
	// Caller返回函数的调用信息，具体包括程序计数器，文件名，行号和是否成功
	_, file, line, ok := runtime.Caller(callerDepth)
	if ok {
		// Base returns the last element of path.
		formattedMsg = fmt.Sprintf("[%s][%s:%d] %s", levelFlags[level], filepath.Base(file), line, msg)
	} else {
		formattedMsg = fmt.Sprintf("[%s] %s", levelFlags[level], msg)
	}
	// 从Pool中重新利用一个logEntry词条
	entry := logger.entryPool.Get().(*logEntry)
	entry.msg = formattedMsg
	entry.level = level
	logger.entryChan <- entry
}

// Debug logs debug message through DefaultLogger
func Debug(v ...any) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(DEBUG, defaultCallerDepth, msg)
}

// Debugf logs debug message through DefaultLogger
func Debugf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(DEBUG, defaultCallerDepth, msg)
}

// Info logs message through DefaultLogger
func Info(v ...any) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(INFO, defaultCallerDepth, msg)
}

// Infof logs message through DefaultLogger
func Infof(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(INFO, defaultCallerDepth, msg)
}

// Warn logs warning message through DefaultLogger
func Warn(v ...any) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(WARNING, defaultCallerDepth, msg)
}

// Error logs error message through DefaultLogger
func Error(v ...any) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(ERROR, defaultCallerDepth, msg)
}

// Errorf logs error message through DefaultLogger
func Errorf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(ERROR, defaultCallerDepth, msg)
}

// Fatal prints error message then stop the program
func Fatal(v ...any) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(FATAL, defaultCallerDepth, msg)
}
