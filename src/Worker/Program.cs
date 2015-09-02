using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkerLib {
    /**
     * The Workers decide between themselves the division of the input file into S splits of equal size.
     * Once assigned a task, the worker asks the client for the split, providing the start and end position.
     * Each line of the split is run through the map function provided by the user, returning a set of key-value pairs.
     * After all the workers are done, they submit the results to the client.
     **/
    class Program {
        static void Main(string[] args) {
        }
    }
}
