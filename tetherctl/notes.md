# Notes on how to make config file setup for tether

## when this is a library
* the .service file is added automatically
* \<interface-name\>.config files are expected to be placed in /etc/library-name/

## interface-up command
* need user to be superuser (either throw error or prompt user to give privilages)
* [DONE] check if config file exists
* [DONE] throw error if interface already exists
* (maybe) check ip forwarding status if it is not enabled then enable it temporarily
* add interface %i (based on config use wg-kernel or wireguard-go)
* [DONE] if at any point error occurs the interface needs to be deleted and changes be undone
* [DONE] run preup commands
* [DONE] set config for the interface (listen port, private/public keypair)
* [DONE] add addresses to interface 
* (not for now) set mtu?
* [DONE] bring interface up (ip link set %i up)
* (not for now) set dns?
* (not for now) set routes for allowed-ips?
* [DONE] run postup commands

## interface-down command
* need user to be superuser (either throw error or prompt user to give privilages)
* [DONE] throw error if interface doesnt exist
* [DONE] run predown commands
* [DONE] save config when option is on (i.e, overrides current config)
* (not for now) revert changes done to route
* [DONE] delete interface %i
* (not for now) unset dns
* [DONE] run postdown commands

## config file
* address
* listenport
* option for wg-kernel or wireguard-go?
* keys?
* mtu?
* pre/post up/down
* save config
* peers


## other TODOs
* should address be mandatory in config file
* decide if we need mtu and dns options like wg-quick
* decide if we wanna add a firewall options
* decide if we wanna add fwMark option

## links
* wg-quick manual - https://www.man7.org/linux/man-pages/man8/wg-quick.8.html
* wg manual - https://manpages.debian.org/unstable/wireguard-tools/wg.8.en.html
* vps server setup - https://markliversedge.blogspot.com/2023/09/wireguard-setup-for-dummies.html


## current workflow 
### file structure
* /etc/tetherctl/ contains tetherctl binary
* /etc/systemd/system contains by-wireguard\@.service file

### assumptions
* wireguard-go is available

### each time service changes (inside connect/tetherctl run)
* sudo cp by-wireguard\@.service /etc/systemd/system/
* systemctl daemon-reload
* systemctl start by-wireguard@wg0
* sudo journalctl -fu by-wireguard@wg0.service
* systemctl stop by-wireguard@wg0
* sudo journalctl -fu by-wireguard@wg0.service

### each time tetherctl changes (inside connect/tetherctl run)
* go build -o tetherctl main.go
* sudo mv tetherctl /etc/tetherctl/

### create symbolic link to be able to execute "tehterctl" command in terminal
* sudo ln -s /etc/tetherctl/tetherctl /usr/local/bin/tetherctl


