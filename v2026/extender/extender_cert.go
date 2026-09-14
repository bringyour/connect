package extender

import (
	"container/list"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/urnetwork/connect/v2026"
)

// Extender certificates (EXTENDER.md B3).
//
// The extender terminates the outer TLS for every server name, so it needs a
// certificate for a name it does not own. With an identity key it signs a
// self-signed ed25519 ca once and issues a leaf per name under it, which is
// what a client with a verified record checks: the leaf's signature must
// verify under the identity key, with no chain building and no roots. Without
// an identity key the leaf is self-signed, and a client has nothing to check.
//
// One ecdsa P-256 leaf key serves the whole process. Leaves are cached per
// name with a bounded lru, which replaces the per-connection rsa-2048
// generation of the v1 server: a handshake no longer pays for a key.
//
// The type is safe for concurrent use.

// Validity of the identity ca.
const extenderCaValidFor = 10 * 365 * 24 * time.Hour

// Validity of a leaf issued under the identity ca.
const extenderLeafValidFor = 30 * 24 * time.Hour

// Cached leaves, evicting the least recently used.
const extenderLeafCacheCount = 1024

// Subject organization of the leaf issued for a handshake that carried no sni,
// which is what a dial presents while the spoof list is empty (A10). A leaf
// needs some identity, and there is no name to derive one from: the extender
// address is not a name the client asked for, and an empty subject with no san
// is not a certificate a verifying client would accept.
const extenderUnnamedOrganization = "Extender"

type extenderCertificates struct {
	stateLock sync.Mutex

	// nil when the extender has no identity key
	identityPrivateKey ed25519.PrivateKey
	caCertificate      []byte

	leafPrivateKey *ecdsa.PrivateKey

	// name to the cache entry holding its leaf, ordered most recent first
	serverNameEntries map[string]*list.Element
	recentServerNames *list.List

	validFrom time.Duration
	validFor  time.Duration

	// test seam; nil in production
	certificateHandler func(serverName string)
}

// One cache entry, so eviction can find the name from the order list.
type extenderCertificateEntry struct {
	serverName  string
	certificate *tls.Certificate
}

// Builds the certificate issuer. An empty seed leaves the extender without an
// identity, which is the manually configured extender of v1.
func newExtenderCertificates(identityKeySeed []byte, settings *ExtenderSettings) (*extenderCertificates, error) {
	leafPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	certificates := &extenderCertificates{
		leafPrivateKey:     leafPrivateKey,
		serverNameEntries:  map[string]*list.Element{},
		recentServerNames:  list.New(),
		validFrom:          settings.ValidFrom,
		validFor:           settings.ValidFor,
		certificateHandler: settings.CertificateHandler,
	}
	if 0 < len(identityKeySeed) {
		identityPrivateKey, err := connect.ExtenderPrivateKeyFromSeed(identityKeySeed)
		if err != nil {
			return nil, err
		}
		caCertificate, err := certificates.newCaCertificate(identityPrivateKey)
		if err != nil {
			return nil, err
		}
		certificates.identityPrivateKey = identityPrivateKey
		certificates.caCertificate = caCertificate
	}
	return certificates, nil
}

// The published identity key, or nil when the extender has none.
func (self *extenderCertificates) PublicKey() []byte {
	if self.identityPrivateKey == nil {
		return nil
	}
	return []byte(self.identityPrivateKey.Public().(ed25519.PublicKey))
}

// Signs a probe challenge, or returns nil without an identity key.
func (self *extenderCertificates) SignChallenge(challenge []byte) []byte {
	if self.identityPrivateKey == nil || len(challenge) == 0 {
		return nil
	}
	return connect.SignExtenderChallenge(self.identityPrivateKey, challenge)
}

// The tls callback for every carrier. A name is issued once and reused.
func (self *extenderCertificates) GetCertificate(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	if self.certificateHandler != nil {
		self.certificateHandler(clientHello.ServerName)
	}
	return self.certificateForServerName(clientHello.ServerName)
}

func (self *extenderCertificates) certificateForServerName(serverName string) (*tls.Certificate, error) {
	cached := func() *tls.Certificate {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		element, ok := self.serverNameEntries[serverName]
		if !ok {
			return nil
		}
		self.recentServerNames.MoveToFront(element)
		return element.Value.(*extenderCertificateEntry).certificate
	}()
	if cached != nil {
		return cached, nil
	}

	certificate, err := self.issue(serverName)
	if err != nil {
		return nil, err
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if element, ok := self.serverNameEntries[serverName]; ok {
		// another handshake issued the same name first; keep one identity
		self.recentServerNames.MoveToFront(element)
		return element.Value.(*extenderCertificateEntry).certificate, nil
	}
	element := self.recentServerNames.PushFront(&extenderCertificateEntry{
		serverName:  serverName,
		certificate: certificate,
	})
	self.serverNameEntries[serverName] = element
	for extenderLeafCacheCount < self.recentServerNames.Len() {
		oldest := self.recentServerNames.Back()
		if oldest == nil {
			break
		}
		self.recentServerNames.Remove(oldest)
		delete(self.serverNameEntries, oldest.Value.(*extenderCertificateEntry).serverName)
	}
	return certificate, nil
}

// The identity ca: self-signed ed25519, regenerated whenever the key changes
// because it is derived from the key alone.
func (self *extenderCertificates) newCaCertificate(identityPrivateKey ed25519.PrivateKey) ([]byte, error) {
	serialNumber, err := newCertificateSerialNumber()
	if err != nil {
		return nil, err
	}
	notBefore := time.Now().Add(-self.validFrom)
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Extender"},
			CommonName:   "Extender Root",
		},
		NotBefore:             notBefore,
		NotAfter:              time.Now().Add(extenderCaValidFor),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	return x509.CreateCertificate(
		rand.Reader,
		&template,
		&template,
		identityPrivateKey.Public(),
		identityPrivateKey,
	)
}

// Issues one leaf, under the identity ca when there is one and self-signed
// otherwise. An empty name is a handshake that sent no sni (A10): it gets a
// leaf with the placeholder subject and no san, which every carrier serves
// like any other.
func (self *extenderCertificates) issue(serverName string) (*tls.Certificate, error) {
	serialNumber, err := newCertificateSerialNumber()
	if err != nil {
		return nil, err
	}
	organization := guessOrganizationName(serverName)
	if organization == "" {
		organization = extenderUnnamedOrganization
	}
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{organization},
		},
		NotBefore:             time.Now().Add(-self.validFrom),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	if serverName != "" {
		if ip := net.ParseIP(serverName); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, serverName)
		}
	}

	var parent *x509.Certificate
	var signer crypto.Signer
	var chain [][]byte
	if self.identityPrivateKey != nil {
		caCertificate, err := x509.ParseCertificate(self.caCertificate)
		if err != nil {
			return nil, err
		}
		template.NotAfter = time.Now().Add(extenderLeafValidFor)
		parent = caCertificate
		signer = self.identityPrivateKey
		chain = [][]byte{self.caCertificate}
	} else {
		// the v1 shape: a self-signed leaf a client cannot check, valid for
		// the configured window
		template.NotAfter = time.Now().Add(self.validFor)
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
		parent = &template
		signer = self.leafPrivateKey
	}

	leafDer, err := x509.CreateCertificate(
		rand.Reader,
		&template,
		parent,
		self.leafPrivateKey.Public(),
		signer,
	)
	if err != nil {
		return nil, err
	}
	leafCertificate, err := x509.ParseCertificate(leafDer)
	if err != nil {
		return nil, err
	}
	return &tls.Certificate{
		Certificate: append([][]byte{leafDer}, chain...),
		PrivateKey:  self.leafPrivateKey,
		Leaf:        leafCertificate,
	}, nil
}

func newCertificateSerialNumber() (*big.Int, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, fmt.Errorf("could not generate a certificate serial number: %w", err)
	}
	return serialNumber, nil
}

// A standalone self-signed leaf, used by the certificate issuer's fixtures and
// by callers that want one certificate without an extender.
func selfSignedCertificate(
	hosts []string,
	organization string,
	validFrom time.Duration,
	validFor time.Duration,
) (*tls.Certificate, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serialNumber, err := newCertificateSerialNumber()
	if err != nil {
		return nil, err
	}
	// ValidFrom is the tolerated clock-skew/history window before creation;
	// ValidFor is the future lifetime after creation.
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{organization},
		},
		NotBefore:             time.Now().Add(-validFrom),
		NotAfter:              time.Now().Add(validFor),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, host)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, privateKey.Public(), privateKey)
	if err != nil {
		return nil, err
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  privateKey,
		Leaf:        certificate,
	}, nil
}
