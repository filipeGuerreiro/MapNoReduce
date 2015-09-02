using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CommonLib {

    public delegate IWorker CreateWorker(int id, string serviceURL, string entryURL = null);

    public delegate IClient CreateClient(string serviceURL);

    public delegate IPuppetMaster CreatePuppetMaster(string serviceURL);

    public class Factory {
        public static readonly Factory Instance = new Factory();

        private LoggerWrapper logger;

        private Factory() {
            logger = new LoggerWrapper("SERVICE-FACTORY");
        }

        /// <summary>
        /// Tries to retrieve a service at the given url.
        /// </summary>
        /// <param name="serviceType"></param>
        /// <param name="url"></param>
        /// <returns>The service if successful, null otherwise</returns>
        public Service GetService(Type serviceType, string url) {
            try {
                // TODO: be sure the url is correct and contains an explicit port
                Regex regex = RegexConstants.PATTERN_PORT;
                var v = regex.Match(url);
                int port = int.Parse(v.Groups[1].ToString());

                Service res = (Service)Activator.GetObject(serviceType, url + port);
                res.ProbeObject();
                return res;
            } catch (Exception e) {
                logger.Log("Error retriving service: " + e.Message, false);
            }

            return null;
        }

        /// <summary>
        /// Creates a service.
        /// </summary>
        /// <param name="port"></param>
        /// <param name="name"></param>
        /// <param name="service">The service instance</param>
        /// <param name="type">The service class type, use typeof(ClassName)</param>
        /// <returns>True if succeed, False otherwise!</returns>
        public bool CreateService(int port, string name, Service service, Type type, string url = null) {
            try {
                BinaryServerFormatterSinkProvider provider = new BinaryServerFormatterSinkProvider();
                provider.TypeFilterLevel = TypeFilterLevel.Full; // this is used to bypass authentication exceptions
                IDictionary props = new Hashtable();
                props["port"] = port;
                props["name"] = name + port;

                TcpChannel channel = new TcpChannel(props, null, provider);
                ChannelServices.RegisterChannel(channel, true);
                RemotingServices.Marshal(service, name + port, type);

                service.Channel = channel;

                // TODO: set to the global ip address
                if (url != null) {
                    service.SetServiceUrl(url);
                } else {
                    service.SetServiceUrl("tcp://localhost:" + port + "/" + name);
                }

                return true;
            } catch (SocketException e) {
                logger.Log("Error opening tcp channel [name="
                    + name + ", port=" + port + "]: " + e.Message, false);
            } catch (RemotingException e) {
                logger.Log("Error creating service [name="
                    + name + ", port=" + port + "]: " + e.Message, false);
            }

            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="service"></param>
        /// <returns></returns>
        public bool DestroyService(Service service) {
            try {
                TcpChannel channel = service.Channel;

                RemotingServices.Disconnect(service);
                ChannelServices.UnregisterChannel(channel);

                return true;
            } catch (SocketException e) {
                logger.Log("Error destroing tcp channel: " + e.Message, false);
            } catch (RemotingException e) {
                logger.Log("Error destroing service: " + e.Message, false);
            }

            return false;
        }

        public bool SafelyUseService<T>(T service, Action<T> a) where T : Service {
            try {
                service.ProbeObject();
                a(service);
                return true;
            } catch(Exception) {
                return false;
            }
        }
    }
}
