/*
Copyright 2021 Oliver Heimlich <oheim@posteo.de>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/* 
Moves the sunscreen or window in the desired position.

Listen on UDP port 8888 for commands 'window open', 'window close', 'curtain open', and 'curtain close'.

The user may override the curtain position via PINs A2 and A3 (e. g. install a
wall switch).

The remote control for the window is connected to PINs 2/3.
The remote controls for the curtains is connected to PINs 5/6 and 8/9.
*/

#include <Ethernet.h>
#include <EthernetUdp.h>

const byte MAC[] = { 0x12, 0x06, 0x69, 0x7d, 0xf7, 0x10 };
const unsigned int LISTEN_PORT = 8888;

const int OPTO_DOWN[] = {2, 5, 8};
const int OPTO_UP[] = {3, 6, 9};
const int MANUAL_DOWN = A3;
const int MANUAL_UP = A2;

enum OperationMode { automatic, manual_down, manual_up };
enum Position { unknown, up, down };

Position automaticPosition[] = {unknown, unknown, unknown};
Position currentPosition[] = {unknown, unknown, unknown};
OperationMode curtainOpMode = automatic;

char packetBuffer[UDP_TX_PACKET_MAX_SIZE];

EthernetUDP udp;

void setup() {
  for (int idx = 0; idx < (sizeof(OPTO_DOWN) / sizeof(int)); idx++) {
    pinMode(OPTO_DOWN[idx], OUTPUT);
    pinMode(OPTO_UP[idx], OUTPUT);
  }

  Ethernet.begin(MAC);

  udp.begin(LISTEN_PORT);
}

void loop() {
  Ethernet.maintain();

  updateAutomaticPosition();

  curtainOpMode = getOpModeStable();

  for (int idx = 0; idx < (sizeof(OPTO_DOWN) / sizeof(int)); idx++) {
    setPosition(idx, getTargetPosition(idx));
  }
}

// Receive commands as UDP packets and set the automaticPosition[] accordingly
void updateAutomaticPosition() {
  while (true) {
    int packetSize = udp.parsePacket();
    if (packetSize == 0) {
      return;
    }
    
    udp.read(packetBuffer, UDP_TX_PACKET_MAX_SIZE);
    if (packetSize < UDP_TX_PACKET_MAX_SIZE) {
      packetBuffer[packetSize] = 0x00;
    }

    if (strcmp(packetBuffer, "window open") == 0) {
      automaticPosition[0] = up;
    } else if (strcmp(packetBuffer, "window close") == 0) {
      automaticPosition[0] = down;
    } else if (strcmp(packetBuffer, "curtain open") == 0) {
      automaticPosition[1] = up;
      automaticPosition[2] = up;
    } else if (strcmp(packetBuffer, "curtain close") == 0) {
      automaticPosition[1] = down;
      automaticPosition[2] = down;
    } else {
      // unsupported command
    }
  }
}

// Determine the target position for a remote control,
// based on the wall switch and the last received command.
Position getTargetPosition(int idx) {
  OperationMode opMode;
  if (idx == 0) {
    // The first remote control moves the window position,
    // which can not be overridden by the wall switch.
    opMode = automatic;
  } else {
    opMode = curtainOpMode;
  }

  switch (opMode) {
    case manual_down:
      return down;
    case manual_up:
      return up;
    case automatic:
      return automaticPosition[idx];
    default:
      return unknown;
  }
}

// Read input pins until two subsequent measurements are equal.
OperationMode getOpModeStable() {
  OperationMode opMode = getOpMode();
  OperationMode previousOpMode;
  do {
    delay(200);
    previousOpMode = opMode;
    opMode = getOpMode();
  } while (previousOpMode != opMode);
  return opMode;
}

// Read input pins until none or only one pin is in HIGH state.
OperationMode getOpMode() {
  while (true) {
    switch (((digitalRead(MANUAL_UP) == HIGH) << 1) | (digitalRead(MANUAL_DOWN) == HIGH)) {
      case 0:
        return automatic;
      case 1:
        return manual_down;
      case 2:
        return manual_up;
      case 3:
        // Illegal state: Both up and down buttons are pressed
        ;
    }
    delay(200);
  }
}

// Activate the remote control to move the window / curtain into a new position.
void setPosition(int idx, Position target) {
  if (target == currentPosition[idx]) {
    // nothing to do
    return;
  }

  int opto;
  switch (target) {
    case up:
      opto = OPTO_UP[idx];
      break;
    case down:
      opto = OPTO_DOWN[idx];
      break;
    default:
      // target == unknown
      return;
  }

  // Send signal to move curtain
  digitalWrite(opto, HIGH);
  delay(200);
  digitalWrite(opto, LOW);

  // Wait until signal has been sent to avoid interference
  // delay(500);

  currentPosition[idx] = target;
}
