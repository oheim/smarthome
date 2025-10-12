let deviceInfo = Shelly.getDeviceInfo();
let deviceName = deviceInfo['name'];

function updateMeasurement() {
  let component = "switch:0";
  let switchStatus = Shelly.getComponentStatus(component);
  let totalEnergy = switchStatus['aenergy']['total']; // active energy counter / Total energy consumed in Watt-hours
  
  console.log("total energy: " + JSON.stringify(totalEnergy) + " Wh");
    
  if (MQTT.isConnected()) {
    MQTT.publish('shellies/energy/total/' + deviceName, JSON.stringify(totalEnergy), 1, true);
  }
}

Timer.set(
   60 * 1000, // every minute
   true, // repeat
   updateMeasurement
 );