using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client.Connecting;
using MQTTnet.Client.Disconnecting;
using MQTTnet.Client.Options;
using MQTTnet.Client.Receiving;
using MQTTnet.Extensions.ManagedClient;
using Newtonsoft.Json;
using Serilog;

namespace MqttTestCore
{
    class Program
    {
        static string CLIENT_ID = "mqtttestcli";
        static string HOSTNAME = "localhost";
        static string TOPIC = "application/1/device/+/event/up";
        static int HOST_PORT = 1883;
        static double CONN_TRY_GAP = 10;
        static async Task Main(string[] args)
        {
            await do_work();
        }

        public static void OnConnected(MqttClientConnectedEventArgs obj)
        {
            Log.Logger.Information("Successfully connected.");
        }

        public static void OnConnectingFailed(ManagedProcessFailedEventArgs obj)
        {
            Log.Logger.Warning("Connection Failed!");
        }

        public static void OnDisconnected(MqttClientDisconnectedEventArgs obj)
        {
            Log.Logger.Information("Disconnected. Trying to re-connect...");
        }

        public static async Task do_work()
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            MqttClientOptionsBuilder builder = new MqttClientOptionsBuilder()
                                        .WithClientId(CLIENT_ID)
                                        .WithTcpServer(HOSTNAME, HOST_PORT);

            ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                                    .WithAutoReconnectDelay(TimeSpan.FromSeconds(CONN_TRY_GAP))
                                    .WithClientOptions(builder.Build())
                                    .Build();

            IManagedMqttClient _mqttClient = new MqttFactory().CreateManagedMqttClient();

            _mqttClient.ConnectedHandler = new MqttClientConnectedHandlerDelegate(OnConnected);
            _mqttClient.DisconnectedHandler = new MqttClientDisconnectedHandlerDelegate(OnDisconnected);
            _mqttClient.ConnectingFailedHandler = new ConnectingFailedHandlerDelegate(OnConnectingFailed);

            _mqttClient.ApplicationMessageReceivedHandler = new MqttApplicationMessageReceivedHandlerDelegate(a => {
                string payload = Encoding.Default.GetString(a.ApplicationMessage.Payload);
                Log.Logger.Information("Message recieved: {payload}", payload);
            });

            _mqttClient.StartAsync(options).GetAwaiter().GetResult();

            while (true)
            {
                MqttTopicFilter filter = new MqttTopicFilterBuilder().WithTopic(TOPIC).WithExactlyOnceQoS().Build();

                await _mqttClient.SubscribeAsync(filter);

                await Task.Delay(1000);
            }
        }
    }
}
