# GoClusterFS Alpine VM Image
# Packer template using pre-built Alpine cloud image (more reliable than ISO install)
#
# Usage:
#   export GOCLUSTERFS_BINARY=/path/to/goclusterfs-mount
#   export SSH_PUBLIC_KEY="ssh-ed25519 AAAA..."
#   export OUTPUT_DIR=/path/to/output
#   packer build alpine.pkr.hcl

packer {
  required_plugins {
    qemu = {
      version = ">= 1.0.0"
      source  = "github.com/hashicorp/qemu"
    }
  }
}

# Variables
variable "alpine_version" {
  type    = string
  default = "3.19"
}

variable "output_directory" {
  type    = string
  default = env("OUTPUT_DIR")
}

variable "goclusterfs_binary" {
  type    = string
  default = env("GOCLUSTERFS_BINARY")
}

variable "ssh_public_key" {
  type    = string
  default = env("SSH_PUBLIC_KEY")
}

variable "disk_size" {
  type    = string
  default = "1G"
}

variable "memory" {
  type    = string
  default = "512"
}

# Local variables
locals {
  # Alpine provides pre-built cloud images in QCOW2 format
  cloud_image_url = "https://dl-cdn.alpinelinux.org/alpine/v${var.alpine_version}/releases/x86_64/alpine-virt-${var.alpine_version}.0-x86_64.iso"
  output_dir      = var.output_directory != "" ? var.output_directory : "${path.root}/../runtime/packer-output"
}

# QEMU builder - use a simpler approach with direct console access
source "qemu" "alpine" {
  iso_url          = local.cloud_image_url
  iso_checksum     = "file:https://dl-cdn.alpinelinux.org/alpine/v${var.alpine_version}/releases/x86_64/alpine-virt-${var.alpine_version}.0-x86_64.iso.sha256"
  output_directory = local.output_dir
  vm_name          = "goclusterfs-alpine.qcow2"

  # VM configuration
  disk_size      = var.disk_size
  memory         = var.memory
  cpus           = 1
  accelerator    = "kvm"
  format         = "qcow2"
  disk_interface = "virtio"
  net_device     = "virtio-net"

  # Boot settings - simplified for reliability
  boot_wait = "30s"
  boot_command = [
    # Login as root (no password on live system)
    "root<enter><wait5>",

    # Set up basic networking
    "setup-interfaces -a<enter><wait5>",
    "ifup eth0<enter><wait5>",

    # Set root password for SSH
    "echo 'root:testpass123' | chpasswd<enter><wait>",

    # Enable and configure SSH
    "apk add openssh<enter><wait10>",
    "sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config<enter><wait>",
    "sed -i 's/#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config<enter><wait>",
    "rc-service sshd start<enter><wait5>",
    "rc-update add sshd<enter><wait>",

    # Set up disk for installation
    "setup-disk -m sys -s 0 /dev/vda<enter><wait5>",
    "y<enter><wait60>",

    # Reboot into installed system
    "reboot<enter><wait60>",

    # Login after reboot
    "root<enter><wait>",
    "testpass123<enter><wait5>",

    # Re-enable SSH after reboot
    "rc-service sshd start<enter><wait5>",
  ]

  # SSH configuration for provisioning
  ssh_username     = "root"
  ssh_password     = "testpass123"
  ssh_timeout      = "15m"
  shutdown_command = "poweroff"

  # Headless mode
  headless = true

  # QEMU display
  display = "none"
}

# Build
build {
  sources = ["source.qemu.alpine"]

  # Install required packages
  provisioner "shell" {
    inline = [
      "apk update",
      "apk add fuse3 fuse3-dev util-linux procps",
      "modprobe fuse || true",
      "echo 'fuse' >> /etc/modules",
    ]
  }

  # Set up SSH key authentication
  provisioner "shell" {
    inline = [
      "mkdir -p /root/.ssh",
      "chmod 700 /root/.ssh",
      "echo '${var.ssh_public_key}' > /root/.ssh/authorized_keys",
      "chmod 600 /root/.ssh/authorized_keys",
    ]
  }

  # Copy GoClusterFS binary
  provisioner "file" {
    source      = var.goclusterfs_binary
    destination = "/usr/local/bin/goclusterfs-mount"
  }

  provisioner "shell" {
    inline = [
      "chmod +x /usr/local/bin/goclusterfs-mount",
    ]
  }

  # Copy entrypoint script
  provisioner "file" {
    source      = "${path.root}/../scripts/vm-entrypoint.sh"
    destination = "/usr/local/bin/vm-entrypoint.sh"
  }

  provisioner "shell" {
    inline = [
      "chmod +x /usr/local/bin/vm-entrypoint.sh",
    ]
  }

  # Set up auto-start of GoClusterFS
  provisioner "shell" {
    inline = [
      # Create init script for GoClusterFS
      "cat > /etc/init.d/goclusterfs << 'INITEOF'\n#!/sbin/openrc-run\n\ndepend() {\n    need localmount\n    after modules\n}\n\nstart() {\n    ebegin \"Starting GoClusterFS\"\n    /usr/local/bin/vm-entrypoint.sh &\n    eend $?\n}\n\nstop() {\n    ebegin \"Stopping GoClusterFS\"\n    fusermount -u /mnt/goclusterfs 2>/dev/null || true\n    eend $?\n}\nINITEOF",
      "chmod +x /etc/init.d/goclusterfs",
      "rc-update add goclusterfs default",

      # Create mount point
      "mkdir -p /mnt/goclusterfs",

      # Clean up
      "rm -f /root/.ash_history",
    ]
  }
}
