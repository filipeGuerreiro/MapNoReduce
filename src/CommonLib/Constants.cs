using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CommonLib {

    public class RegexConstants {

        // String formats
        public static readonly Regex PATTERN_URL = new Regex(@"\w+:\/\/[\w\.]+(:\d+)?\/?.*");
        public static readonly Regex PATTERN_ID = new Regex(@"\d+");
        public static readonly Regex PATTERN_SECONDS = new Regex(@"\d+");
        public static readonly Regex PATTERN_FILE = new Regex(@"[\w\\:]+");
        public static readonly Regex PATTERN_IGNORE = new Regex(@"\s*%.*");
        public static readonly Regex PATTERN_PORT = new Regex(@".+:(\d+)/.*");
    }

    public static class Constants {
        public const string LOCALHOST_URL = "tcp://localhost";

        public const int CLIENT_PORT = 10001;
        public const string CLIENT_SERVICE_NAME = "C";
        public const string CLIENT_URL = "tcp://localhost:10001/C";

        public const int PUPPETMASTER_PORT_START = 20001;
        public const string PUPPETMASTER_SERVICE_NAME = "PM";

        public const int WORKER_PORT_START = 30001;
        public const string WORKER_SERVICE_NAME = "W";

        // Commands strings
        public const string COMMAND_ON_LINE = "On line {0}:";
        public const string COMMAND_NOT_FOUND = "Not a valid command.";
        public const string COMMAND_END_OF_SCRIPT = "Program reached end of script.";
        public const string COMMAND_HELP = "WORKER <ID> <PUPPETMASTER-URL> <SERVICE-URL> <ENTRY-URL>\r\n"
            + "SUBMIT <ENTRY-URL> <FILE> <OUTPUT> <S> <MAP>\r\n"
            + "WAIT <SECS>\r\n"
            + "STATUS\r\n"
            + "SLOWW <ID> <delay-in-seconds>\r\n"
            + "FREEZEW <ID>\r\n"
            + "UNFREEZEW <ID>\r\n"
            + "FREEZEC <ID>\r\n"
            + "UNFREEZEC <ID>";

        // worker
        public const string COMMAND_WORKER_INVALID_ENTRY_URL =
            "WORKER: <ENTRY-URL> format is incorrect.";
        public const string COMMAND_WORKER_INVALID_PARAMETERS =
            "WORKER: not valid parameter format. Please check the <ID> and URLs and try again.";
        public const string COMMAND_WORKER_INVALID_SYNTAX =
            "Invalid syntax! Syntax is: \r\nWORKER <ID> <PUPPETMASTER-URL> <SERVICE-URL> <ENTRY-URL>";

        // submit
        public const string COMMAND_SUBMIT_INVALID_PARAMETERS =
            "SUBMIT: not valid parameter format. Please check the inputs and try again.";
        public const string COMMAND_SUBMIT_INVALID_SYNTAX =
            "Invalid syntax! Syntax is: \r\nSUBMIT <ENTRY-URL> <FILE> <OUTPUT> <S> <MAP> <DLL>";

        // wait
        public const string COMMAND_WAIT_INVALID_DELAY =
            "WAIT: \'{0}\' is not in a valid format. Please provide an integer instead.";

        // sloww
        public const string COMMAND_SLOWW_INVALID_DELAY =
            "SLOWW: \'{0}\' is not in a valid format. Please provide an integer instead.";
        public const string COMMAND_SLOWW_INVALID_ID =
            "SLOWW: \'{0}\' is not in a valid <ID>. Please provide an integer instead.";

        // freezew
        public const string COMMAND_FREEZEW_INVALID_ID =
            "FREEZEW: \'{0}\' is not in a valid <ID>. Please provide an integer instead.";

        // unfreezew
        public const string COMMAND_UNFREEZEW_INVALID_ID =
            "UNFREEZEW: \'{0}\' is not in a valid <ID>. Please provide an integer instead.";

        // freezec
        public const string COMMAND_FREEZEC_INVALID_ID =
            "FREEZEC: \'{0}\' is not in a valid <ID>. Please provide an integer instead.";

        // unfreezec
        public const string COMMAND_UNFREEZEC_INVALID_ID =
            "UNFREEZEC: \'{0}\' is not in a valid <ID>. Please provide an integer instead.";

        // freezew
        public const string COMMAND_KILL_INVALID_ID =
            "KILL: \'{0}\' is not in a valid <ID>. Please provide an integer instead.";
    }
}
