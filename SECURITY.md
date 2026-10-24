# Security Policy

## Supported versions

GoClusterFS has no release yet. Only the `main` branch gets security fixes. This section will list supported releases after the first one ships.

## Report a vulnerability

Do not report security problems in public issues, pull requests, or discussions.

Report them privately through GitHub:

1. Open the repository's **Security** tab.
2. Click **Report a vulnerability**.
3. Fill in the form.

Only you and the maintainers can see the report. We reply and work on the fix with you there.

If the **Report a vulnerability** button is missing, open an issue that asks a maintainer to contact you. Leave out all details of the problem.

## What to include

- The commit you tested (`git rev-parse HEAD`)
- What an attacker can do, and what access they need (for example, a local user on a node, or a host on the broadcast network)
- Steps to reproduce, or a proof of concept
- Kernel version, shared storage type, and mount flags, if they matter
