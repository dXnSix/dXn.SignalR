using System.Collections.Concurrent;
using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;

namespace dXn.SignalR.TestService
{
    /// <summary>
    /// Test-only rotating tokens for the <c>token-factory</c> scenario: <c>/e2e/token</c> (and
    /// <see cref="TestHub.IssueToken"/>) issues a token of the current generation, and
    /// <c>/e2e/rotate-token</c> (and <see cref="TestHub.RotateTokens"/>) starts a new generation, after
    /// which every earlier token is rejected. A client that reconnects after a rotation therefore
    /// proves it fetched a fresh token.
    /// </summary>
    public static class RotatingTokens
    {
        static readonly ConcurrentDictionary<string, int> _issued = new(StringComparer.Ordinal);
        static int _generation = 1;

        public static string Issue()
        {
            int generation = Volatile.Read(ref _generation);
            string token = $"rotating-{generation}-{Guid.NewGuid():N}";
            _issued[token] = generation;
            return token;
        }

        public static int Rotate() => Interlocked.Increment(ref _generation);

        /// <summary>Whether <paramref name="token"/> was issued for the current generation.</summary>
        public static bool TryValidate(string token, out int generation) =>
            _issued.TryGetValue(token, out generation) && generation == Volatile.Read(ref _generation);

        public static void Reset()
        {
            _issued.Clear();
            Interlocked.Exchange(ref _generation, 1);
        }
    }

    /// <summary>
    /// Test-only authentication for <c>/test-token</c>: accepts a current-generation token from
    /// <see cref="RotatingTokens"/> as a Bearer header or the <c>access_token</c> query.
    /// </summary>
    public sealed class RotatingTokenAuthenticationHandler(
        IOptionsMonitor<AuthenticationSchemeOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
    {
        public const string SchemeName = "E2ERotatingToken";
        public const string GenerationClaim = "e2e-token-generation";

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            string? header = Request.Headers.Authorization;
            string? token = header is not null && header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)
                ? header["Bearer ".Length..].Trim()
                : Request.Query["access_token"].ToString();
            if (string.IsNullOrEmpty(token))
            {
                return Task.FromResult(AuthenticateResult.NoResult());
            }

            if (!RotatingTokens.TryValidate(token, out int generation))
            {
                return Task.FromResult(AuthenticateResult.Fail("The token is unknown or was rotated"));
            }

            var identity = new ClaimsIdentity(
                [new Claim(ClaimTypes.Name, "e2e-rotating"), new Claim(GenerationClaim, generation.ToString(System.Globalization.CultureInfo.InvariantCulture))],
                "Bearer");
            return Task.FromResult(AuthenticateResult.Success(
                new AuthenticationTicket(new ClaimsPrincipal(identity), SchemeName)));
        }
    }

    /// <summary>Mapped at <c>/test-token</c>: requires a current rotating token.</summary>
    [Authorize(AuthenticationSchemes = RotatingTokenAuthenticationHandler.SchemeName)]
    public sealed class TokenHub : Hub
    {
        /// <summary>The generation of the token the connection authenticated with.</summary>
        public int TokenGeneration() =>
            int.Parse(
                Context.User?.FindFirst(RotatingTokenAuthenticationHandler.GenerationClaim)?.Value ?? "0",
                System.Globalization.CultureInfo.InvariantCulture);

        /// <summary>
        /// Severs the physical transport (no SignalR Close), so the client's reconnection policy
        /// reconnects with a fresh negotiate.
        /// </summary>
        public void Abort() => TransportFaults.Sever(Context);
    }
}
