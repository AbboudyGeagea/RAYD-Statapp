# Microsoft 365 setup for RAYD scheduled emails (OAuth)

**For:** LAUMC IT / Microsoft 365 administrator

RAYD emails scheduled reports through Microsoft 365. It runs unattended, so it signs in as a
registered application instead of with a user's password. Microsoft turns password sign-in for
SMTP off by default at the end of December 2026.

## Check this first

**Security defaults must be off.** While Microsoft Entra *security defaults* is enabled, SMTP AUTH
is disabled in Exchange Online, with OAuth too. Microsoft's guidance is to disable security defaults
to use SMTP AUTH. If LAUMC will not do that, tell us before doing the steps below: RAYD would need
to send through Microsoft Graph instead, which is a code change on our side.

## What we need from you

| Item | Where it comes from |
|---|---|
| Directory (tenant) ID | App registration, Overview page |
| Application (client) ID | App registration, Overview page |
| Client secret **Value**, and its expiry date | Step 3. The Value, not the Secret ID |
| The sending mailbox address | Step 1 |
| Confirmation of the firewall rules | Step 7 |

Send the secret through a secure channel, not by email.

## Steps

**1. Choose a dedicated mailbox** for RAYD to send from, for example `rayd-reports@<domain>`. Step 5
gives the app full access to this mailbox, so don't use a person's.

**2. Register an application** in Microsoft Entra: *App registrations > New registration*, with
*Accounts in this organizational directory only*. No redirect URI is needed.

**3. Create a client secret:** *Certificates & secrets > New client secret*. Copy the **Value** right
away. Sending stops when it expires, so diary the renewal.

**4. Add the permission:** *API permissions > Add a permission > APIs my organization uses*, search
for **Office 365 Exchange Online**, then *Application permissions > **SMTP.SendAsApp***. Then choose
**Grant admin consent**.

**5. Register the app in Exchange and give it the mailbox,** in Exchange Online PowerShell:

```powershell
Install-Module -Name ExchangeOnlineManagement
Connect-ExchangeOnline -Organization <tenantId>

New-ServicePrincipal -AppId <APPLICATION_ID> -ObjectId <OBJECT_ID>
Get-ServicePrincipal | Format-List

Add-MailboxPermission -Identity "<mailbox>" -User <SERVICE_PRINCIPAL_ID> -AccessRights FullAccess
```

`OBJECT_ID` is the Object ID on the **Enterprise applications** Overview page for this app. It is
**not** the Object ID under App registrations. Using the wrong one makes sign-in fail later.

**6. Allow SMTP AUTH on that one mailbox.** This overrides the organization setting, so SMTP AUTH can
stay off for everyone else:

```powershell
Set-CASMailbox -Identity "<mailbox>" -SmtpClientAuthenticationDisabled $false
```

**7. Open the firewall,** outbound only, from the RAYD server `10.4.15.121`:

| Destination | Port | Purpose |
|---|---|---|
| `login.microsoftonline.com` | 443 | Getting the sign-in token |
| `smtp.office365.com` | 587 | Sending the mail (STARTTLS) |

## In RAYD

A RAYD admin opens *Report Composer > Scheduled emails > Mail server*, chooses **Microsoft 365
(OAuth)**, fills in the IDs and secret, sets the From address to the mailbox from step 1, lists the
allowed recipient domains, and uses **Save and send a test email**.

## If the test email fails

| RAYD says | Usual cause |
|---|---|
| `Microsoft refused the sign-in: AADSTS...` | Wrong tenant or client ID, the Secret ID instead of its Value, an expired secret, or no admin consent (step 4). The AADSTS code says which |
| `Microsoft issued a token but the mail server refused it` (535) | Security defaults on, SMTP AUTH off for the mailbox (step 6), or step 5 incomplete, often the wrong Object ID. Permissions can take a while to apply, so retry after waiting |
| `SendAsDenied` | The From address is not the mailbox from step 1 |
| Timed out, or could not connect | The firewall rules in step 7 |

## Microsoft's documentation

- [Authenticate an IMAP, POP or SMTP connection using OAuth](https://learn.microsoft.com/en-us/exchange/client-developer/legacy-protocols/how-to-authenticate-an-imap-pop-smtp-application-by-using-oauth)
- [Enable or disable SMTP AUTH in Exchange Online](https://learn.microsoft.com/en-us/exchange/clients-and-mobile-in-exchange-online/authenticated-client-smtp-submission)
- [Updated SMTP AUTH basic authentication deprecation timeline](https://techcommunity.microsoft.com/blog/exchange/updated-exchange-online-smtp-auth-basic-authentication-deprecation-timeline/4489835)
