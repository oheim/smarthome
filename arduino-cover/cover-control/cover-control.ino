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

   
Moves the curtain in the desired position.

Listen on UDP port 8888 for commands 'open' and 'close'.

The user may override the curtain position via PINs 6 and 8.

The remote control for the curtain is connected to PINs 2 and 5.

*/

#include <Ethernet.h>
#include <EthernetUdp.h>
#include <string.h>

const byte MAC[] = { 0x12, 0x06, 0x69, 0x7d, 0xf7, 0x10 };
const unsigned int LISTEN_PORT = 8888;

const int SIGNAL_LED = 7;
const int OPTO_DOWN = 2;
const int OPTO_UP = 5;
const int MANUAL_DOWN = 6;
const int MANUAL_UP = 8;

enum OperationMode { automatic, manual_down, manual_up };
enum CurtainPosition { unknown, up, down };

CurtainPosition automaticPosition = unknown;
CurtainPosition currentPosition = unknown;

char packetBuffer[UDP_TX_PACKET_MAX_SIZE];

EthernetUDP udp;

void setup() {
  pinMode(SIGNAL_LED, OUTPUT);
  pinMode(OPTO_UP, OUTPUT);
  pinMode(OPTO_DOWN, OUTPUT);
  pinMode(MANUAL_UP, INPUT);
  pinMode(MANUAL_DOWN, INPUT);

  digitalWrite(SIGNAL_LED, HIGH);

  Ethernet.begin(MAC);

  udp.begin(LISTEN_PORT);

  digitalWrite(SIGNAL_LED, LOW);
}

void loop() {
  Ethernet.maintain();

  updateAutomaticPosition();

  moveCurtain(getTargetPosition());
}

void updateAutomaticPosition() {
  int packetSize = udp.parsePacket();
  if (packetSize) {
    udp.read(packetBuffer, UDP_TX_PACKET_MAX_SIZE);

    if (strncmp(packetBuffer, "open", packetSize) == 0) {
      automaticPosition = up;
    }

    if (strncmp(packetBuffer, "close", packetSize) == 0) {
      automaticPosition = down;
    }
  }
}

CurtainPosition getTargetPosition() {
  OperationMode opMode = automatic;
  if (digitalRead(MANUAL_UP) == HIGH) {
    opMode = manual_up;
  } else {
    if (digitalRead(MANUAL_DOWN) == HIGH) {
      opMode = manual_down;
    }
  }

  switch (opMode) {
    case manual_down:
      return down;
    case manual_up:
      return up;
    case automatic:
      return automaticPosition;
    default:
      return unknown;
  }
}

void moveCurtain(CurtainPosition target) {
  if (target == currentPosition) {
    // nothing to do
    return;
  }

  int opto;
  switch (target) {
    case up:
      opto = OPTO_UP;
      break;
    case down:
      opto = OPTO_DOWN;
      break;
    default:
      return;
  }

  // Send signal to move curtain
  digitalWrite(SIGNAL_LED, HIGH);
  digitalWrite(opto, HIGH);
  delay(200);
  digitalWrite(opto, LOW);
  digitalWrite(SIGNAL_LED, LOW);

  currentPosition = target;
}
