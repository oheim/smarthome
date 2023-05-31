// Periodically publish the power level of the PV device via MQTT.
// The power level is proportional to the current amount of solar irradiation.
//
// This script is supposed to run on a Shelly Gen2 device,
// which monitors the energy production of a PV device.

let averagePower = 0.0; // in Watt, exponential smoothing
let smoothingFactor = 0.125;
let deviceInfo = Shelly.getDeviceInfo();
let deviceName = deviceInfo['name'];

function updateMeasurement() {
  let component = "switch:0";
  let switchStatus = Shelly.getComponentStatus(component);
  let energyLastMinute = switchStatus['aenergy']['by_minute'][1]; // in Milliwatt-hours, last full minute

  let averagePowerLastMinute = energyLastMinute * 60 / 1000; // in Watt

  // MQTT.publish(deviceName + "/status/" + component + "/power/average", JSON.stringify(averagePowerLastMinute), 1, false);

  averagePower += smoothingFactor * (averagePowerLastMinute - averagePower);

  MQTT.publish(deviceName + "/status/" + component + "/power/average_smooth", JSON.stringify(Math.round(averagePower)), 1, false);
}

Timer.set(
   60000, // every minute
   true, // repeat
   updateMeasurement
 );
