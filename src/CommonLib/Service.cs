using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;

namespace CommonLib {
    public class Service : MarshalByRefObject {

        private string serviceURL;
        public string ServiceURL {
            get { return serviceURL; }
        }

        private TcpChannel channel;
        internal TcpChannel Channel {
            get { return channel; }
            set { channel = value; }
        }

        internal void SetServiceUrl(string serviceURL) {
            this.serviceURL = serviceURL;
        }

        public bool ProbeObject(bool test = true) {
            return test;
        }

        // No lease time (NullPointerException on tracker)
        //public override object InitializeLifetimeService() {
        //    return null;
        //}
    }
}
