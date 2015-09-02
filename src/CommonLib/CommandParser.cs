using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CommonLib {

    public delegate void ProcessResult(IDictionary<string, string> parameters, int line);

    public struct Command {
        private string name;
        public string Name {
            get { return name; }
            set { name = value.ToUpperInvariant(); }
        }
        public ProcessResult Callback;
        public IList<string> ParametersName;

        public Command(string name, ProcessResult callback, params string[] parametersName) {
            this.name = name.ToUpperInvariant();
            Callback = callback;
            ParametersName = new List<string>(parametersName);
        }
    }

    public class CommandParser {
        private IDictionary<String, Command> commands;

        private ProcessResult commandNotFound;
        private ProcessResult endOfScript;

        private string script;
        private string[] scriptLines = { };
        private int scriptCurrentLine;
        public string Script {
            get { return script; }
            set {
                script = value;
                // TODO: reiniciar os steps ou continuar quando se muda o script?
                scriptCurrentLine = 0;
                scriptLines = script.Split(new string[] { "\n", "\r\n" },
                    StringSplitOptions.RemoveEmptyEntries);
            }
        }

        public CommandParser(ProcessResult commandNotFound = null, ProcessResult endOfScript = null) {
            commands = new Dictionary<String, Command>();
            this.commandNotFound = commandNotFound;
            this.endOfScript = endOfScript;
        }

        public void AddCommand(Command cmd) {
            commands.Add(cmd.Name, cmd);
        }

        public void ConsoleParser() {
            int lineNumber = 0;
            do {
                lineNumber += 1;
                string line = Console.ReadLine();
                ParseLine(line, lineNumber);
            } while (true);
        }

        public void ParseScript(string script) {
            string[] lines = script.Split(new string[] { "\n", "\r\n" }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < lines.Length; i++) {
                string line = lines[i];
                ParseLine(line, i + 1);
            }
        }

        public void ParseScript() {
            for (int i = 0; i < scriptLines.Length; i++) {
                string line = scriptLines[i];
                ParseLine(line, i + 1);
            }
        }

        public void ParseLine(string line, int number) {
            string[] splitedLine = Regex.Split(line, @"\s+");

            if (splitedLine.Length > 0) {
                // Is comment?
                if (splitedLine[0].StartsWith("%")) {
                    return;
                }

                string command = splitedLine[0].ToUpperInvariant();

                if (commands.ContainsKey(command)) {
                    Command c = commands[command];
                    IDictionary<string, string> parameters = new Dictionary<string, string>();
                    for (int i = 0; i < c.ParametersName.Count; i++) {
                        if (i < splitedLine.Length - 1) {
                            parameters.Add(c.ParametersName[i], splitedLine[i + 1]);
                        } else {
                            parameters.Add(c.ParametersName[i], "");
                        }
                    }
                    c.Callback(parameters, number);
                    return;
                }
                // Command not found
                if (commandNotFound != null) { commandNotFound(null, number); }
            }
        }

        public void ParseLine(string line) {
            if (line.Equals("")) {
                return;
            }
            ParseLine(line, scriptCurrentLine + 1);
        }

        public void ParseLine() {
            // Reached end of script
            if (scriptCurrentLine >= scriptLines.Length) {
                if (endOfScript != null) { endOfScript(null, scriptCurrentLine); }
            } else {
                ParseLine(scriptLines[scriptCurrentLine], scriptCurrentLine + 1);
                scriptCurrentLine += 1;
            }
        }
    }

    //public class GUICommands {
    //    public static CommandParser GetCommandParser() {
    //        CommandParser ret = new CommandParser();

    //        ret.AddCommand(new Command("WORKER", new ProcessResult(), "id", "puppetmaster-url", "service-url", "entry-url"));
    //        ret.AddCommand(new Command("SUBMIT", new ProcessResult(), "id", "entry-url", "file", "output", "s", "map", "dll"));
    //        ret.AddCommand(new Command("WAIT", new ProcessResult(), "delay"));
    //        ret.AddCommand(new Command("STATUS", new ProcessResult()));
    //        ret.AddCommand(new Command("SLOWW", new ProcessResult(), "id", "delay"));
    //        ret.AddCommand(new Command("FREEZEW", new ProcessResult(), "id"));
    //        ret.AddCommand(new Command("UNFREEZEW", new ProcessResult(), "id"));
    //        ret.AddCommand(new Command("FREEZEC", new ProcessResult(), "id"));
    //        ret.AddCommand(new Command("UNFREEZEC", new ProcessResult(), "id"));
    //        ret.AddCommand(new Command("HELP", new ProcessResult()));

    //        return ret;
    //    }
    //}
}
