using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace dXn.SignalR.TestService
{
    /// <summary>
    /// Wraps a hub protocol and counts the SignalR Close messages clients send, so the
    /// graceful-stop scenario can prove that <c>stop</c> sends one before the transport closes
    /// (ASP.NET Core reports a plain WebSocket close to <c>OnDisconnectedAsync</c> the same way).
    /// </summary>
    public sealed class CloseCountingHubProtocol(IHubProtocol inner) : IHubProtocol
    {
        public string Name => inner.Name;

        public int Version => inner.Version;

        public TransferFormat TransferFormat => inner.TransferFormat;

        public bool TryParseMessage(
            ref ReadOnlySequence<byte> input,
            IInvocationBinder binder,
            [NotNullWhen(true)] out HubMessage? message)
        {
            if (!inner.TryParseMessage(ref input, binder, out message))
            {
                return false;
            }

            if (message is CloseMessage)
            {
                TestControls.CountClientClose();
            }

            return true;
        }

        public void WriteMessage(HubMessage message, IBufferWriter<byte> output) =>
            inner.WriteMessage(message, output);

        public ReadOnlyMemory<byte> GetMessageBytes(HubMessage message) => inner.GetMessageBytes(message);

        public bool IsVersionSupported(int version) => inner.IsVersionSupported(version);
    }
}
