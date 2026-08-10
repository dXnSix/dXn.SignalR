using Microsoft.AspNetCore.SignalR;
using System.Diagnostics;

namespace SignalRTestService
{
    public class TestHub : Hub
    {
        public async Task<TestEntity> SingleEntity()
        {
            await Task.CompletedTask;

            return new TestEntity
            {
                Number = 1,
                Text = "test"
            };
        }

        public string ConnectionIdentity() => Context.ConnectionId;

        public async Task AbortPhysicalTransport()
        {
            var httpContext = Context.GetHttpContext()
                ?? throw new InvalidOperationException("SignalR transport has no HTTP context");

            // Let the stateful protocol acknowledge the test command before severing only the
            // underlying HTTP transport. The logical SignalR connection remains resumable.
            await Task.Delay(1200, Context.ConnectionAborted);
            httpContext.Abort();
        }

        public async IAsyncEnumerable<int> OrderedSequence(int count, int delayMilliseconds = 1)
        {
            for (var value = 1; value <= count; value++)
            {
                if (Context.ConnectionAborted.IsCancellationRequested)
                {
                    yield break;
                }

                if (delayMilliseconds > 0)
                {
                    await Task.Delay(delayMilliseconds, Context.ConnectionAborted);
                }
                yield return value;
            }
        }

        public async Task TriggerOrderedCallbacks(string callback, int count)
        {
            for (var value = 1; value <= count; value++)
            {
                await Clients.Caller.SendAsync(callback, value, Context.ConnectionAborted);
            }
        }

        public async IAsyncEnumerable<TestEntity> HundredEntities()
        {
            for(var i = 0; i < 100; i ++)
            {
                await Task.CompletedTask;

                yield return new TestEntity
                {
                    Number = i + 1,
                    Text = $"test {i + 1}"
                };
            }
        }

        public async IAsyncEnumerable<TestEntity> MillionEntities()
        {
            for (var i = 0; i < 100000; i++)
            {
                await Task.CompletedTask;

                if (Context.ConnectionAborted.IsCancellationRequested)
                {
                    yield break;
                }

                yield return new TestEntity
                {
                    Number = i + 1,
                    Text = $"test {i + 1}"
                };
            }
        }

        public async Task<bool> PushEntity(TestEntity entity)
        {
            await Task.CompletedTask;

            return true;
        }

        public async Task<TestEntity> PushTwoEntities(TestEntity entity1, TestEntity entity2)
        {
            await Task.CompletedTask;

            return new TestEntity
            {
                Text = $"{entity1.Text}_{entity2.Text}",
                Number = entity1.Number + entity2.Number,
            };
        }

        public async Task TriggerCallback(string callback)
        {
            await Clients.Caller.SendAsync(callback);
        }

        public async Task TriggerEntityCallback(string callback)
        {
            await Clients.Caller.SendAsync(callback, new TestEntity
            {
                Text = "callback",
                Number = 1,
            });
        }

        public async Task<bool> TriggerEntityResponse(string callback)
        {
            var ret = await Clients.Caller.InvokeAsync<TestEntity>(callback, new TestEntity
            {
                Text = "requested response",
                Number = 1,
            }, CancellationToken.None);

            if(ret.Number == 1)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            Debug.WriteLine($"Client is disconnected: {Context.ConnectionId}");

            return base.OnDisconnectedAsync(exception);
        }

        public override Task OnConnectedAsync()
        {
            Debug.WriteLine($"Client is connecting: {Context.ConnectionId}");

            return base.OnConnectedAsync();
        }
    }

    public class TestEntity
    {
        public int Number { get; set; }
        public string Text { get; set; } = string.Empty;
    }
}
