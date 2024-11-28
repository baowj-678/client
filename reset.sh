#!/bin/bash

rm file
sudo rm log/*
sudo rm  /var/run/dragonfly/peer-dev.sock
sudo rm -r /var/lib/dragonfly-peer-dev/metadata/*
sudo rm -r /var/lib/dragonfly-peer-dev/content/*
