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
// Additionally, an error will be returned if the ByWgConfig.Name does not match the Client.DeviceName.
func (c *Client) BringUpInterface(bywgConf ByWgConfig) error {
	// TODO: need user to be superuser (either throw error or prompt user to give privilages)

	if c.DeviceName != bywgConf.Name {
		return fmt.Errorf("name in config does not match the device name")
	}

	errorOccurred := true
	defer func() {
		if errorOccurred {
			c.revertUpChanges()
		}
	}()

	if err := c.createWgInterface(); err != nil {
		return fmt.Errorf("could not create WireGuard interface: %w", err)
	}

	runCommands(bywgConf.PreUp, c.DeviceName)
	if err := c.setupDevice(bywgConf.PrivateKey, bywgConf.ListenPort, bywgConf.Peers); err != nil {
		return fmt.Errorf("device setup failed: %w", err)
	}
	c.addAddresses(bywgConf.Address)
	if err := c.runUpCommand(); err != nil {
		return fmt.Errorf("error running up command: %w", err)
	}
	runCommands(bywgConf.PostUp, c.DeviceName)

	errorOccurred = false // do not execute defer statement
	return nil
}

// BringDownInterface removes a WireGuard interface based on a configuration file.
//
// bywgConf is the configuration file.
// configSavePath is the location where the updated configuration file will be stored.
//
// The function returns an error if the interface could not be brought down.
// Additionally, an error will be returned if the Config.Name does not match the Client.DeviceName.
func (c *Client) BringDownInterface(bywgConf ByWgConfig, configSavePath string) error {
	// TODO: need user to be superuser (either throw error or prompt user to give privilages)

	if c.DeviceName != bywgConf.Name {
		return fmt.Errorf("name in config does not match the device name")
	}

	// throw error if device does not exist or is not a WireGuard device
	if _, err := c.Device(c.DeviceName); err != nil {
		return fmt.Errorf("could not get device: %w", err)
	}

	runCommands(bywgConf.PreDown, c.DeviceName)
	if bywgConf.SaveConfig {
		c.SaveConfigToFile(bywgConf, configSavePath)
	}
	if err := c.runDownCommand(); err != nil {
		return fmt.Errorf("error running down command: %w", err)
	}
	runCommands(bywgConf.PostDown, c.DeviceName)

	return nil
}

func (c *Client) revertUpChanges() {
	fmt.Println("Error bringing up the interface. Reverting changes by deleting the interface...")
	if err := c.runDownCommand(); err != nil {
		fmt.Println(err)
	}
}

func (c *Client) createWgInterface() error {
	// _, err := c.Device(c.DeviceName)
	// if err == nil {
	// 	return fmt.Errorf("device %q already exists", c.DeviceName)
	// }

	// _, err = exec.LookPath("wireguard-go")
	// if err != nil {
	// 	return fmt.Errorf("failed to find wireguard-go: %w", err)
	// }

	// fmt.Printf("%s wireguard-go %s\n", TETHER_CMD, c.DeviceName)
	// cmd := exec.Command("wireguard-go", c.DeviceName)
	// if err := cmd.Run(); err != nil {
	// 	return fmt.Errorf("failed to create WireGuard interface: %w", err)
	// }

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
func (c *Client) setupDevice(privKey string, listenPort *int, peers []wgtypes.PeerConfig) error {
	privateKey, err := wgtypes.ParseKey(privKey)
	if err != nil {
		return fmt.Errorf("failed to parse private key: %w", err)
	}

	// If ListenPort is nil then random one should be chosen, however,
	// ListenPort is initially chosen at random when creating interface so no need to override here

	err = c.ConfigureDevice(c.DeviceName, wgtypes.Config{
		PrivateKey:   &privateKey,
		ListenPort:   listenPort, // if nil it is not applied
		ReplacePeers: true,
		Peers:        peers,
	})
	if err != nil {
		return fmt.Errorf("failed to configure device: %w", err)
	}

	return nil
}

func (c *Client) addAddresses(addresses []string) {
	commands := []string{}
	for _, address := range addresses {
		protoVersion := map[bool]string{true: "6", false: "4"}[strings.Contains(address, ":")]
		cmd := fmt.Sprintf("ip -%s address add %s dev %s", protoVersion, address, c.DeviceName)
		commands = append(commands, cmd)
	}
	runCommands(commands, c.DeviceName)
}

func (c *Client) runUpCommand() error {
	cmd := fmt.Sprintf("ip link set %s up", c.DeviceName)
	fmt.Printf("%s %s\n", TETHER_CMD, cmd)
	output, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to bring up interface: %w: %s", err, output)
	}
	return nil
}

func (c *Client) runDownCommand() error {
	cmd := fmt.Sprintf("ip link delete dev %s", c.DeviceName)
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
