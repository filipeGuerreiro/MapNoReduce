using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommonLib {

    [Serializable]
    public struct split {
        public int id;
        public int from;
        public int to;

        public split(int id, int from, int to) {
            this.id = id;
            this.from = from;
            this.to = to;
        }

        public void ForEach(IList<string> list, Action<int, string> iterator) {
            for (int i = from; i <= to; i++) {
                iterator(i, list[i]);
            }
        }
    }

    [Serializable]
    public class JobAssignment {

        private int workerId = -1;
        public int WorkerId {
            get { return workerId; }
            set { workerId = value; }
        }

        private List<split> jobs;
        public List<split> Jobs {
            get { return jobs; }
        }

        public JobAssignment(int workerId, List<split> jobs) {
            this.workerId = workerId;
            this.jobs = jobs;
        }
    }
}
