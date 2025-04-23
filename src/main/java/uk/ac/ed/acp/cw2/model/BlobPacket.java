package uk.ac.ed.acp.cw2.model;

public class BlobPacket {
    public String uid = "s2093547";
    public String datasetName;
    public String data;
    public String uuid;

    public BlobPacket(String datasetName, String data) {
        this.datasetName = datasetName;
        this.data = data;
        this.uuid = null;
    }

    public BlobPacket(String uuid) {
        this.uuid = uuid;
        this.datasetName = null;
        this.data = null;
    }

    public BlobPacket() {
        this.datasetName = null;
        this.data = null;
        this.uuid = null;
    }

    public String pushJson() {
        return "{ \"uid\": \"" + uid + "\", \"datasetName\": \"" + datasetName + "\", \"data\": \"" + data + "\" }";
    }

    public String receiveJson() {
        return "{ \"uuid\": \"" + uuid + "\", \"datasetName\": \"" + datasetName + "\", \"data\": \"" + data + "\" }";
    }

    public BlobPacket copy(){
        BlobPacket copy = new BlobPacket();
        copy.uid = uid;
        copy.datasetName = datasetName;
        copy.data = data;
        copy.uuid = uuid;
        return copy;
    }
}
