# tetherctl
A way to manage wireguard interfaces as well as create your own without the need for a kernel module.

## 1. Assumptions
* [wireguard-go](https://github.com/WireGuard/wireguard-go) is available as a command. To do so follow [instructions](https://github.com/WireGuard/wireguard-go) on how to build and place the created binary in your $PATH.

## 2. Using the package locally
```bash
go run main.go --help # to see cli options
```
or check [CLI options](#cli-options)

## 3. Using the package as a service
1. compile package and move to `/etc/tetherctl/`
```bash
go build -o tetherctl main.go
sudo mkdir -p /etc/tetherctl/ 
sudo mv tetherctl /etc/tetherctl/
```
2. make config file (`bywg0.conf`) and place it in `/etc/tetherctl/`
```bash
cp bywg0.conf /etc/tetherctl/bywg0.conf
```
Note: check [Config File](#config-file) for more details on how a config file is structured.

3. place service file in systemd
```bash
sudo cp by-wireguard\@.service /etc/systemd/system/
systemctl daemon-reload
```
4. enable the service to run on startup
```bash
systemctl enable by-wireguard@bywg0
```
5. start the service
```bash
systemctl start by-wireguard@bywg0
```
Note: *use `sudo journalctl -fu by-wireguard@bywg0` to see logs of the service.*

## 4. CLI options
Note: *Check help menu for default values of* [optional] *arguments!*
* `tetherctl up [--device_name=<device_name>]  [--config_file=<config_file>]` - starts a wireguard device with the given name and config file. Config file specifies the path to the folder where the config is found, i.e., for `<device_name>=bywg0` and `<config_file>=/etc/tetherctl/` the configuration should be placed in `/etc/tetherctl/bywg0.conf`.
* `tetherctl down [--device_name=<device_name>] [--config_file=<config_file>] [--new_file=<new_file>]` - removes a wireguard device with the given name and config file.
* `tetherctl get-config [--device_name=<device_name>] [--config_file=<config_file>]` - prints the config of a WireGuard device with the given name and config file. The config is given with its initial values but updated based on the peers, listen port, and keypair of the device.
* `tetherctl save-config [--device_name=<device_name>] [--config_file=<config_file>] [--new_file=<new_file>]` - saves the updated config (check previous bullet point) of a WireGuard device with the given name and config file. The config is saved in the given new file.
* `tetherctl gen-priv-key` - returns a private key for a WireGuard device.
* `tetherctl gen-pub-key --priv_key=<priv_key>` - returns the public key for a given private key.
* `tetherctl get-device-names` - returns the names of all available WireGuard devices.
* `tetherctl get-device [--device_name=<device_name>]` - returns the struct of a WireGuard device as given by `wgtypes.Device` (this includes type of device).
* `tetherctl change-device [--device_name=<device_name>] [--listen_port=<listen_port>] [--priv_key=<priv_key>]` - changes the listen port and/or private key of a WireGuard device.
* `tetherctl add-peer [--device_name=<device_name>] [--endpoint=<endpoint>] --pub_key=<pub_key>` - adds a peer to a WireGuard device with the given public key and endpoint (public IP of the server where it can be contacted). If the peer already exists it just adds a new IP to its AllowedIPs. The config that the peer can use to setup its own wireguard cilent is returned here.
* `tetherctl get-peer-config [--device_name=<device_name>] [--endpoint=<endpoint>] --pub_key=<pub_key>` - returns the config of a peer with the given public key and endpoint (same config as `add-peer`).
* `tetherctl start-api [--device_name=<device_name>] [--endpoint=<endpoint>] [--api_url=<api_url>]` - starts an API server that can be used to manage the WireGuard device. The API server is started on the given URL which must include a port. Check [API](#api) for more details.

## 5. API
To start the api use the `tetherctl start-api` command. By default the API is set on `localhost:9090` but if you want to expose it to be accessible through your public ip you can set `<api_url>=:9090`.

 The API has the following endpoints:
 * POST `/peer/add/*pubKey` - adds a peer with the given public key to the WireGuard device. The request has no body. The config that the peer can use to setup its own wireguard cilent is returned here (essentially runs `add-peer` command).
* GET `/peer/config/*pubKey` - returns the config of a peer with the given public key (essentially runs `get-peer-config` command).


## 6. Example 
Assuming you have a WireGuard interface "bywg0" setup on server (can use `up` command or kernel module to set it up). Check [Links](#links) for more details on some tutorials on how to setup a WireGuard interface using the kernel module.
1. Start API on server
```bash
go run main.go start-api --device_name=bywg0 --api_url=:9090
```
2. On peer machine run
```bash
go run main.go gen-priv-key
go run main.go gen-pub-key --priv_key=<private-key-from-above>
```
3. Use Postman (or any other way) to make POST command `http://<public-ip-of-server>:9090/peer/add/<public-key-from-above>`
4. Get output from POST similar to:
```ini
[Interface]
PrivateKey = __PLACEHOLDER__ # replace __PLACEHOLDER__ with your private key
Address = 192.168.89.1/32
DNS = 1.1.1.1

[Peer]
PublicKey = <public-key-of-server>
AllowedIPs = 0.0.0.0/0
Endpoint = <endpoint-of-server>
```

5. Replace the `__PLACEHOLDER__` with the key generated from step 2 and use the config in any WireGuard client to connect to the server.

## 7. Config File
The config file is a simple `ini` formatted file similar to the ones used by [wg-quick](https://www.man7.org/linux/man-pages/man8/wg-quick.8.html) and the [wg kernel module](https://www.man7.org/linux/man-pages/man8/wg.8.html). The config file can have two sections:
* `[Interface]` - contains the configuration for the interface (mandatory, exactly one). The following options are available:
  * `Address` - a comma-separated list of IPs in CIDR notation to be assigned to the interface (can appear multiple times).
  * `ListenPort` - the port on which the interface listens.
  * `PrivateKey` - the private key of the interface (mandatory).
  * `PreUp`, `PostUp`, `PreDown`, `PostDown` - bash commands which will be executed before/after setting up/tearing down the interface (can appear multiple times). The special string `%i' is expanded to the interface name.
  * `SaveConfig` - a boolean value to save the config of the interface when being brought down. Any changes made to device while the interface is up will be saved to the config file.
* `[Peer]` - contains the configuration for a peer (optional, can appear multiple times). The following options are available:
  * `PublicKey` - the public key of the peer (mandatory).
  * `AllowedIPs` - a comma-separated list of IPs in CIDR notation that the peer is allowed to access through the interface (can appear multiple times).
  * `Endpoint` - the public IP of the server where the peer can be contacted.


### Example Config
```ini
[Interface]
Address = 192.168.89.1/24
ListenPort = 33333
PrivateKey = __PLACEHOLDER__ # replace __PLACEHOLDER__ with your private key 
SaveConfig = true
PostUp = iptables -A FORWARD -i %i -j ACCEPT; iptables -A FORWARD -o %i -j ACCEPT; iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE
PostDown = iptables -D FORWARD -i %i -j ACCEPT; iptables -D FORWARD -o %i -j ACCEPT; iptables -t nat -D POSTROUTING -o eth0 -j MASQUERADE 
# here eth0 is the interface where the public ip of the server is assigned

[Peer]
PublicKey = <some-peer-public-key>
AllowedIPs = 192.168.89.101/32
```

## 8. Links
* [How to Set up WireGuard on a VPS Server: A Step-by-Step Guide](https://www.vps-mart.com/blog/how-to-set-up-wireguard-on-VPS)
* [Wireguard setup for dummies](https://markliversedge.blogspot.com/2023/09/wireguard-setup-for-dummies.html)