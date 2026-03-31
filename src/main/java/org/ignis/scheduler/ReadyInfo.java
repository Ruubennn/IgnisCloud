package org.ignis.scheduler;

public class ReadyInfo {
    public String container;
    public String instanceId;
    public String ip;
    public Integer port;
    public String driverIp;

    public boolean isValid() {
        return ip != null && !ip.isBlank() && port != null && port > 0;
    }

    @Override
    public String toString() {
        return "ReadyInfo{container='" + container + "', instanceId='" + instanceId +
                "', ip='" + ip + "', port=" + port + ", driverIp='" + driverIp + "'}";
    }
}
