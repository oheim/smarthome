// Publish start / stop events for the connected device (washing maschine) via MQTT
// This script is supposed to run on a Shelly device, e. g. Shelly PlusPlugS.

let lastState = '';
let seenAsInactive = 0; // number of times the device has been seen below the power threshold
let seenAsActive = 0;

let deviceInfo = Shelly.getDeviceInfo();
let deviceName = deviceInfo['name'];

function updateMeasurement() {
  
  let component = "switch:0";
  let switchStatus = Shelly.getComponentStatus(component);
  let newState = 'stop';
  let power = switchStatus['apower']; // instantaneous active power (in Watts)
  
  if (power > 20) {
    newState = 'start';
    seenAsInactive = 0;
  } else if (power < 3.2) {
    seenAsActive = 0;
  } else {
    return;
  }
  
  // The washer might pause the motor for a few seconds
  // during a cycle.
  //
  // We don't want to falsely detect this as the end
  // of a new cycle.
  if (lastState == 'start' && newState == 'stop' && seenAsInactive < 5) {
    seenAsInactive++;
    newState = lastState;
  } else if (lastState == 'stop' && newState == 'start' && seenAsActive < 5) {
    seenAsActive++;
    newState = lastState;
  } else if (lastState == '' && newState == 'stop') {
    lastState = newState;
  }

  if (newState != lastState && MQTT.isConnected()) {
    MQTT.publish(deviceName + "/status/" + component + "/power/state", newState, 1, true);
    lastState = newState;
  }
}

Timer.set(
   1000, // every second
   true, // repeat
   updateMeasurement
 );

