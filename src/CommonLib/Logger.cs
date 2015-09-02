using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace CommonLib {

    struct Log {
        public string who;
        public string msg;
        public bool printOnConsole;

        public Log(string who, string msg, bool printOnConsole = true) {
            this.who = who;
            this.msg = msg;
            this.printOnConsole = printOnConsole;
        }
    }

    public class LoggerWrapper {
        private string who;

        public LoggerWrapper(string who) {
            this.who = who;
        }

        public void Log(string message) {
            Logger.Instance.Log(who, message);
        }

        public void Log(string message, bool printOnConsole) {
            Logger.Instance.Log(who, message, printOnConsole);
        }
    }

    /// <summary>
    /// Logger class.
    /// 
    /// Simple log system that writes output to
    /// separate files, based on the who parameter.
    /// </summary>
    public class Logger {

        public static readonly Logger Instance = new Logger();

        private string path;

        public bool PrintToConsole = true;

        private Queue<Log> logTask = new Queue<Log>();

        /// <summary>
        /// Initiates a Logger.
        /// </summary>
        /// <param name="who">name for the file, must be accepted by the filesystem</param>
        /// <param name="path">path to file, must end with /</param>
        private Logger(string path = "../../../Logs/") {
            this.path = path;
            Directory.CreateDirectory(path);
            new Thread(new ThreadStart(doLog)).Start();
        }

        /// <summary>
        /// Logs a message to a file, if PrintToConsole is true
        /// the message will be also printed to the console with
        /// the prefix who + "> ".
        /// </summary>
        /// <param name="logMessage">message to log</param>
        public void Log(string who, string logMessage, bool printOnConsole = true) {
            lock (logTask) {
                logTask.Enqueue(new Log(who, logMessage, printOnConsole));
                Monitor.PulseAll(logTask);
            }
        }

        /// <summary>
        /// Same as Log(string). Uses a formated string.
        /// </summary>
        /// <param name="logMessageFormat">message string format</param>
        /// <param name="parameters">format parameters</param>
        public void Log(string who, bool printOnConsole, string logMessageFormat, params object[] parameters) {
            Log(who, string.Format(logMessageFormat, parameters), printOnConsole);
        }

        public static void prompt() {
            Console.Write("$ ");
        }

        private void doLog() {
            do {
                lock (logTask) {
                    if (logTask.Count == 0) {
                        Monitor.Wait(logTask);
                    }
                    Log log = logTask.Dequeue();

                    if(!Directory.Exists(path)) {
                        Directory.CreateDirectory(path);
                    }

                    using (StreamWriter w = File.AppendText(path + "/" + log.who + ".logfile")) {
                        w.Write("\r\nLog Entry : ");
                        w.WriteLine("{0} {1}", DateTime.Now.ToLongTimeString(),
                            DateTime.Now.ToLongDateString());
                        w.WriteLine("  : {0}", log.msg);
                        w.WriteLine("-------------------------------");
                        w.Flush();
                    }
                    if (PrintToConsole && log.printOnConsole) {
                        Console.WriteLine(log.who + "> " + log.msg);
                        prompt();
                    }
                }
            } while (true);
        }
    }
}
