package link;

import java.util.HashMap;

public class Links {
	private HashMap<String, Link> links;

	public Links() {
		this.links = new HashMap<>();
	}

	public Links(HashMap<String, Link> links) {
		this.links = links;
	}

	public HashMap<String, Link> getLinks() {
		return links;
	}

	public void setLinks(HashMap<String, Link> links) {
		this.links = links;
	}

	public void addLink(Link link){
		this.links.put(link.getId(),link);
	}

	public Link getLink(String linkId){
		return this.links.get(linkId);
	}
}
