using Microsoft.AspNetCore.SignalR;

namespace RayX.SignalR.TestService
{
    /// <summary>
    /// Mapped at <c>/test-connected</c>: greets every connection from <c>OnConnectedAsync</c>, as
    /// hubs that push state on connect do, so only callbacks registered before the client starts
    /// receive the greeting.
    /// </summary>
    public sealed class ConnectedHub : Hub
    {
        /// <summary>The name of the client method the greeting calls.</summary>
        public const string GreetingTarget = "connected";

        /// <summary>Sends the connection id to the caller before any hub method runs.</summary>
        public override async Task OnConnectedAsync()
        {
            await base.OnConnectedAsync();
            await Clients.Caller.SendAsync(GreetingTarget, Context.ConnectionId);
        }

        /// <summary>The server connection id, which changes with every start.</summary>
        public string ConnectionIdentity() => Context.ConnectionId;

        /// <summary>Sends a Close message after the delay, then aborts the connection.</summary>
        public void ScheduleServerClose(bool allowReconnect, int delayMilliseconds) =>
            TestControls.ScheduleClose(Context.ConnectionId, allowReconnect, TimeSpan.FromMilliseconds(delayMilliseconds));
    }
}
