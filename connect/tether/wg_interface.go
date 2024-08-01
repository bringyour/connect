package tether

import (
	"fmt"
	"os/exec"
	"strings"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

const TETHER_CMD string = "[sh]"

// BringUpInterface reads the configuration file at filePath, brings up the corresponding WireGuard interface and configures it.
//
// bywgConf is the configuration file.
//
// The function returns an error if the interface could not be brought up.
// Additionally, an error will be returned if the ByWgConfig.Name does not match the TetherClient.DeviceName.
func (server *TetherClient) BringUpInterface(bywgConf ByWgConfig) error {
	// TODO: need user to be superuser (either throw error or prompt user to give privilages)

	if server.DeviceName != bywgConf.Name {
		return fmt.Errorf("name in config does not match the device name")
	}

	if err := server.createWgInterface(); err != nil {
		server.revertUpChanges()
		return err
	}
	// TODO: add option to config for chosing wireguard implementation

	runCommands(bywgConf.PreUp, server.DeviceName)
	if err := server.setupDevice(bywgConf.PrivateKey, bywgConf.ListenPort, bywgConf.Peers); err != nil {
		server.revertUpChanges()
		return err
	}
	server.addAddresses(bywgConf.Address)
	if err := server.runUpCommand(); err != nil {
		server.revertUpChanges()
		return err
	}
	runCommands(bywgConf.PostUp, server.DeviceName)

	return nil
}

// BringDownInterface removes a WireGuard interface based on a configuration file.
//
// bywgConf is the configuration file.
//
// The function returns an error if the interface could not be brought down.
// Additionally, an error will be returned if the Config.Name does not match the TetherClient.DeviceName.
func (server *TetherClient) BringDownInterface(bywgConf ByWgConfig) error {
	// TODO: need user to be superuser (either throw error or prompt user to give privilages)

	if server.DeviceName != bywgConf.Name {
		return fmt.Errorf("name in config does not match the device name")
	}

	// throw error if device does not exist or is not a WireGuard device
	if _, err := server.Device(server.DeviceName); err != nil {
		return err
	}

	runCommands(bywgConf.PreDown, server.DeviceName)
	if err := server.runDownCommand(); err != nil {
		return err
	}
	runCommands(bywgConf.PostDown, server.DeviceName)

	return nil
}

func (server *TetherClient) revertUpChanges() {
	fmt.Println("Error bringing up the interface. Reverting changes by deleting the interface...")
	if err := server.runDownCommand(); err != nil {
		fmt.Println(err)
	}
}

func (server *TetherClient) createWgInterface() error {
	_, err := server.Device(server.DeviceName)
	if err == nil {
		return fmt.Errorf("device %q already exists", server.DeviceName)
	}

	_, err = exec.LookPath("wireguard-go")
	if err != nil {
		return fmt.Errorf("failed to find wireguard-go: %w", err)
	}

	fmt.Printf("%s wireguard-go %s\n", TETHER_CMD, server.DeviceName)
	cmd := exec.Command("wireguard-go", server.DeviceName)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to create WireGuard interface: %w", err)
	}

	return nil
}

// setupDevice configures the WireGuard interface to use the provided private key and listen port.
// From here on this interface can be abstracted away to a Device (see wg_device.go).
// Function assumes the interface was just created.
//
// privKey is mandatory to setup a device.
// listenPort can be nil, indicating that a random one should be used.
//
// The function returns an error if the private key is invalid or the configuration cannot be applied
func (server *TetherClient) setupDevice(privKey string, listenPort *int, peers []wgtypes.PeerConfig) error {
	privateKey, err := wgtypes.ParseKey(privKey)
	if err != nil {
		return fmt.Errorf("failed to parse private key: %w", err)
	}

	// If ListenPort is nil then random one should be chosen, however,
	// ListenPort is initially chosen at random when creating interface so no need to override here

	return server.ConfigureDevice(server.DeviceName, wgtypes.Config{
		PrivateKey:   &privateKey,
		ListenPort:   listenPort, // if nil it is not applied
		ReplacePeers: true,
		Peers:        peers,
	})
}

func (server *TetherClient) addAddresses(addresses []string) {
	commands := []string{}
	for _, address := range addresses {
		protoVersion := map[bool]string{true: "6", false: "4"}[strings.Contains(address, ":")]
		cmd := fmt.Sprintf("ip -%s address add %s dev %s", protoVersion, address, server.DeviceName)
		commands = append(commands, cmd)
	}
	runCommands(commands, server.DeviceName)
}

func (server *TetherClient) runUpCommand() error {
	cmd := fmt.Sprintf("ip link set %s up", server.DeviceName)
	fmt.Printf("%s %s\n", TETHER_CMD, cmd)
	output, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to bring up interface: %w: %s", err, output)
	}
	return nil
}

func (server *TetherClient) runDownCommand() error {
	cmd := fmt.Sprintf("ip link delete dev %s", server.DeviceName)
	fmt.Printf("%s %s\n", TETHER_CMD, cmd)
	output, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to bring down interface: %w: %s", err, output)
	}
	return nil
}

func runCommands(commands []string, ifaceName string) {
	for _, cmd := range commands {
		cmd = strings.Replace(cmd, "%i", ifaceName, -1) // substitute %i with interface name
		fmt.Printf("%s %s\n", TETHER_CMD, cmd)
		output, _ := exec.Command("sh", "-c", cmd).CombinedOutput()
		// errors are printed with normal output
		fmt.Print(string(output))
	}
}
