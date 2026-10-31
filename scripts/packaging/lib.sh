# Shared by the packaging tests' legs (architecture.md §3.3 *Packaging tests*); sourced, not run.

# The person the Dex mock connector signs in carries this group, which the configuration maps to viewer and exporter.
MAPPED_GROUP=authors
# Dex's static-password person, in no group: signed in, admitted to nothing. The hash is bcrypt of "password", Dex's documented example.
UNMAPPED_EMAIL=nobody@example.test
UNMAPPED_HASH='$2a$10$2b2cU8CPhOTaGrs1HRQuAueS7JTT5ZHsHSzYiFPm1leZck7Mc8T4W'
PEOPLE_CLIENT=tracekeep-web
PEOPLE_SECRET=packaging-people-secret
TOOL=SIM-TOOL
DEVICE=eqp

log() { printf '%s packaging: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { log "$*"; exit 1; }

# ca_init dir: a CA under dir (ca.crt, ca.key) for every HTTPS endpoint the tests reach.
ca_init() {
	local dir=$1
	mkdir -p "$dir"
	openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 -subj "/CN=tracekeep packaging CA" \
		-keyout "$dir/ca.key" -out "$dir/ca.crt" 2>/dev/null
}

# ca_leaf dir name san...: a leaf certificate dir/name.crt and its key dir/name.key, signed by the CA, for the DNS names given.
ca_leaf() {
	local dir=$1 name=$2; shift 2
	local sans
	sans=$(printf 'DNS:%s,' "$@")
	openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=$1" -keyout "$dir/$name.key" -out "$dir/$name.csr" 2>/dev/null
	openssl x509 -req -in "$dir/$name.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial -days 2 \
		-extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth\n' "${sans%,}") -out "$dir/$name.crt" 2>/dev/null
	chmod 0644 "$dir/$name.key"
}

# certutil_path: certutil, from the system or, where it lacks one, extracted from Debian's package without root.
certutil_path() {
	if command -v certutil >/dev/null; then command -v certutil; return; fi
	local dir=$WORK/certutil
	if [ ! -x "$dir/usr/bin/certutil" ]; then
		mkdir -p "$dir"
		(cd "$dir" && apt-get download libnss3-tools >/dev/null && dpkg -x libnss3-tools_*.deb .)
	fi
	echo "$dir/usr/bin/certutil"
}

# browser_home dir: a home whose NSS database trusts the CA, for the browser, so it ignores no certificate error.
browser_home() {
	local home=$1 certutil
	certutil=$(certutil_path)
	mkdir -p "$home/.pki/nssdb"
	"$certutil" -d "sql:$home/.pki/nssdb" -N --empty-password
	"$certutil" -d "sql:$home/.pki/nssdb" -A -t "C,," -n "tracekeep packaging CA" -i "$WORK/ca/ca.crt"
}

# dex_config issuer redirect: Dex's configuration: the mock connector (the mapped person), static passwords (the unmapped one),
# the people client with tracekeep's callback, and no approval screen.
dex_config() {
	cat <<YAML
issuer: $1
storage:
  type: memory
web:
  https: 0.0.0.0:5556
  tlsCert: /etc/dex/tls/tls.crt
  tlsKey: /etc/dex/tls/tls.key
oauth2:
  skipApprovalScreen: true
connectors:
- type: mockCallback
  id: mock
  name: Example
enablePasswordDB: true
staticPasswords:
- email: $UNMAPPED_EMAIL
  hash: "$UNMAPPED_HASH"
  username: nobody
  userID: 00000000-0000-4000-8000-000000000001
staticClients:
- id: $PEOPLE_CLIENT
  name: tracekeep
  secret: $PEOPLE_SECRET
  redirectURIs: ["$2"]
YAML
}

# playwright url [args...]: the browser tests against url, trusting the CA, the tests' host names mapped to this host;
# TRACEKEEP_BEFORE_LOGIN and TRACEKEEP_BEFORE_CALLBACK, when the caller sets them, are the crossing test's commands.
playwright() {
	local url=$1 home=$WORK/browser
	shift
	[ -d "$home/.pki" ] || browser_home "$home"
	(cd web && npx playwright install chromium >/dev/null &&
		TRACEKEEP_URL=$url TRACEKEEP_TOOL=$TOOL TRACEKEEP_DEVICE=$DEVICE TRACEKEEP_UNMAPPED_EMAIL=$UNMAPPED_EMAIL \
		TRACEKEEP_HOST_RULES="MAP tracekeep.test 127.0.0.1, MAP dex.test 127.0.0.1" TRACEKEEP_BROWSER_HOME=$home \
		npx playwright test "$@")
}

# material: 32 random bytes, base64, a key's material.
material() { head -c 32 /dev/urandom | base64 -w0; }
