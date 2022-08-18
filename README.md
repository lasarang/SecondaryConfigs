# SecondaryConfigs
Configuration scripts

## Run script.py on startup

1. Clone the repo and open crop_sensing.service file

Now in line 6 and 7 need to replace 'lasarang' -> with_your_own_username

Save changes

2. In the current directory run

```sudo chmod +x script.py```

This is necessary to enable execution for all users 

3. Then, copy the service to the system directory

```sudo cp crop_sensing.service /lib/systemd/system```

4. Restart daemon on your rpi

```sudo systemctl daemon-reload```

5. Enable the new service

From the project directory 

```sudo systemctl enable /lib/systemd/system/crop-sensing.service```

From the system directory

```cd /lib/systemd/system```

```sudo systemctl enable crop-sensing.service```

6. Reboot your pi

```sudo reboot```

Keep in mind that all your hardware is already connected to internet

7. Check the status of the service after boot

```sudo systemctl status crop-sensing.service```

8. For stop the service run

```sudo systemctl disable crop.sensing.service```
