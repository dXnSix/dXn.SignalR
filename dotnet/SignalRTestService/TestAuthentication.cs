using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace SignalRTestService
{
    /// <summary>
    /// Test-only authentication for the <c>/test-auth</c> hub. Accepts Basic
    /// <c>e2e-user:e2e-password</c> and Bearer <c>e2e-token</c> from the Authorization header or the
    /// <c>access_token</c> query. A WebSocket request without credentials is authenticated as the user
    /// who negotiated its connection token, which is how browsers (which cannot set WebSocket
    /// headers) keep a Basic-authenticated connection.
    /// </summary>
    public sealed class TestAuthenticationHandler(
        IOptionsMonitor<AuthenticationSchemeOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
    {
        public const string SchemeName = "E2E";
        public const string User = "e2e-user";
        public const string Password = "e2e-password";
        public const string Token = "e2e-token";
        public const string BearerUser = "e2e-bearer";
        public const string SourceClaim = "e2e-auth-source";

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            string? header = Request.Headers.Authorization;
            if (!string.IsNullOrEmpty(header))
            {
                if (header.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
                {
                    string credentials = DecodeBasic(header["Basic ".Length..]);
                    return Task.FromResult(credentials == $"{User}:{Password}"
                        ? Success(User, "Basic", "header")
                        : AuthenticateResult.Fail("Invalid Basic credentials"));
                }

                if (header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
                {
                    return Task.FromResult(header["Bearer ".Length..].Trim() == Token
                        ? Success(BearerUser, "Bearer", "header")
                        : AuthenticateResult.Fail("Invalid bearer token"));
                }

                return Task.FromResult(AuthenticateResult.Fail("Unsupported authorization scheme"));
            }

            string? accessToken = Request.Query["access_token"];
            if (!string.IsNullOrEmpty(accessToken))
            {
                return Task.FromResult(accessToken == Token
                    ? Success(BearerUser, "Bearer", "access_token")
                    : AuthenticateResult.Fail("Invalid access token"));
            }

            // Test-only: browsers cannot send Authorization on a WebSocket, so this service maps a
            // negotiated connection token back to the user who negotiated it. A standard ASP.NET
            // Core server does not do this; browser clients should use Bearer tokens.
            string? connectionToken = Request.Query["id"];
            if (!string.IsNullOrEmpty(connectionToken)
                && TestControls.TryGetTokenUser(connectionToken, out var negotiated))
            {
                return Task.FromResult(Success(negotiated.Name, negotiated.Scheme, "token-binding"));
            }

            return Task.FromResult(AuthenticateResult.NoResult());
        }

        static AuthenticateResult Success(string name, string authenticationType, string source)
        {
            var identity = new ClaimsIdentity(
                [new Claim(ClaimTypes.Name, name), new Claim(SourceClaim, source)],
                authenticationType);
            return AuthenticateResult.Success(new AuthenticationTicket(new ClaimsPrincipal(identity), SchemeName));
        }

        static string DecodeBasic(string value)
        {
            try
            {
                return Encoding.UTF8.GetString(Convert.FromBase64String(value.Trim()));
            }
            catch (FormatException)
            {
                return string.Empty;
            }
        }
    }
}
