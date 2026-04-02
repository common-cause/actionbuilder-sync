Person Signup Helper

The person signup helper allows you to create or update a person record, as well as optionally tag that person or add connections.

People are deduplicated by their identifiers. A person will be matched by their Action Builder identifier first. If a match is not found, it will attempt to find a match for the other identifiers provided. Note that non-Action Builder identifiers are not deduplicated, therefore it's possible to have multiple matches to a singular identifier. For this reason, we highly recommend using unique identifiers if you decide to have custom identifiers.

Tags, connections, and connection tags can also be added when using the person signup helper. The optional tag array is matched to existing section, fields, and responses in Action Builder. These fields and responses must exist in the Action Builder UI and be added to the campaign before they can be used over the API, unless the action_builder:create_tag field is set to true, in which case a response is created and/or added to the campaign if the field and section match existing fields and sections. Similarly, the person or entity forming the other end of the connection must already exist before being connected. Tags can also be removed through the remove_tags array.

    Endpoint
    Field Names and Descriptions
    Links
    Related Resources
    Scenario: Creating a new person or entity (POST)
    Scenario: Updating an existing person and adding and removing tags (POST)
    Scenario: Creating a new person and connection (POST)

Endpoints and URL Structures

https://[your-sub-domain].actionbuilder.org/api/rest/v1/campaigns/[campaign_id]/people

The person signup helper lives at the endpoint relating to the collection of people.
Back To Top ↑
Field Names and Descriptions

People Fields
Field Name 	Type 	Required on POST 	Description
person 	Person* 		A hash representing person data. You can use any valid fields for person resources. An Action Builder identifier (for updated person) or name or given_name (for new person) is required. See the people document for more information about people.
add_tags 	Taggings* 		An array of hashes representing taggings data. You can use any valid fields for tagging resources. Matched fields will add the chosen response to this entity. Invalid fields are ignored. Please refer to the taggings documentation for required fields. Use the action_builder:create_tag option to create responses in matching fields if they don't exist already.
remove_tags 	Taggings* 		An array of hashes representing taggings data. You can use any valid fields for tagging resources. Matched responses will be removed from the entity. Invalid matches are ignored. Please refer to the taggings documentation for required fields.
add_connections 	Connections* 		An array of hashes representing connections data. You can use any valid fields for connection resources. A person_id is required. The ID must be the internal Action Builder ID without the prefix, and the entity connected to must be of a type that forms a valid connection type for the campaign.

Back To Top ↑
Related Resources

    Campaigns
    People
    Entity Types
    Taggings
    Connections
    Connections Helper


Back To Top ↑
Scenario: Creating a new person (POST)

If you post with an inline person hash, we will create or update the matching person.

When creating a new person, either a given_name or name is required, as determined by the entity type. When updating a person, the Action Builder identifier is required.

This example creates a new person.

Request

						

POST https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people		

Header:
OSDI-API-Token: your_api_key_here

{ 
  "person": {
    "action_builder:entity_type": "Person",
    "given_name": "Axel",
    "family_name": "Smith",
    "action_builder:latest_assessment": 4
  }
}
					
						

					

Response

						

{
  "person": {
    "origin_system": "Action Builder",
    "identifiers": [
      "action_builder:839db4d7-c86d-4329-a92d-e625134fbb3e"
    ],
    "created_date": "2022-01-24T18:23:23.334Z",
    "modified_date": "2022-12-16T15:20:39.770Z",
    "action_builder:entity_type": "person",
    "given_name": "Axel",
    "family_name": "Smith",
    "browser_url": "https://techworkersunited.actionbuilder.org/entity/view/20565/profile?campaignId=11415",
    "action_builder:latest_assessment": 4,
    "action_builder:latest_assessment_created_date": "2022-12-16T15:20:39.770Z",
    "preferred_language": "en",
    "_links": {
      "self": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people/839db4d7-c86d-4329-a92d-e625134fbb3e"
      },
      "action_builder:campaign": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f"
      },
      "action_builder:entity_type": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/entity_types/786888a8-868f-4694-a3ba-1caf0688d0c0"
      },
      "action_builder:connections": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people/839db4d7-c86d-4329-a92d-e625134fbb3e/connections"
      },
      "osdi:taggings": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people/839db4d7-c86d-4329-a92d-e625134fbb3e/taggings"
      }
    }
  }
}


Back To Top ↑
Scenario: Updating an existing person and adding and removing tags (POST)

You can use the person signup helper to update people as well. People are matched by their Action Builder identifier.

You can modify an existing person's postal address, phone number, or email address by using PUT at the person's endpoint and either using the field's identifier or matching by the address, number, or any of the postal address fields. Postal addresses, phone numbers, and email addresses each have their own action_builder:identifier field which can be used to modify that field.

When updating a person's postal address, phone number, or email address you must include the action_builder:identifier in your request or match by address, number, or any of the postal address fields. Otherwise, new fields will be created instead of updating the existing fields.

You can remove a person's assessment by making action_builder:latest_assessment equal to null.

Request

					

POST https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people

Header:
OSDI-API-Token: your_api_key_here

{ 
  "person": {
    "identifiers": [ "action_builder:839db4d7-c86d-4329-a92d-e625134fbb3e" ],
    "action_builder:latest_assessment": null
  },
  "add_tags": [ {
    "action_builder:section": "Personal Info",
    "action_builder:field": "Pronouns",
    "name": "She/Hers"
  } ],
  "remove_tags": [ {
    "action_builder:section": "Personal Info",
    "action_builder:field": "Pronouns",
    "name": "He/Him"
  } ]
}

						
					

				

Response

			 	 

200 OK

Content-Type: application/hal+json
Cache-Control: max-age=0, private, must-revalidate					 
					 
{
  "person": {
    "origin_system": "Action Builder",
    "identifiers": [
      "action_builder:839db4d7-c86d-4329-a92d-e625134fbb3e"
    ],
    "created_date": "2022-01-24T18:23:23.334Z",
    "modified_date": "2022-12-16T15:20:39.770Z",
    "action_builder:entity_type": "person",
    "given_name": "Axel",
    "family_name": "Smith",
    "browser_url": "https://techworkersunited.actionbuilder.org/entity/view/20565/profile?campaignId=11415",
    "preferred_language": "en",
    "_links": {
      "self": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people/839db4d7-c86d-4329-a92d-e625134fbb3e"
      },
      "action_builder:campaign": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f"
      },
      "action_builder:entity_type": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/entity_types/786888a8-868f-4694-a3ba-1caf0688d0c0"
      },
      "action_builder:connections": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people/839db4d7-c86d-4329-a92d-e625134fbb3e/connections"
      },
      "osdi:taggings": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people/839db4d7-c86d-4329-a92d-e625134fbb3e/taggings"
      }
    }
  }
}
				 

			  


Back To Top ↑
Scenario: Creating a New Person and Connection (POST)

When creating a new person, either a given_name or name is required. When updating a person, the Action Builder identifier is required.

The connections helper requires a person_id that relates to an existing entity or person. You can optionally add tags to the connection, using the connection helper format.

Request

					

POST https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people

Header:
OSDI-API-Token: your_api_key_here

{ 
  "person": {
    "action_builder:entity_type": "Person",
    "given_name": "Sunny",
    "family_name": "Smith"
  },
  "add_connections": [{
    "person_id": "a58866ed-a9a1-493e-a24c-e807e4f890b8",
	"add_tags": [{
		"action_builder:section": "Connection Between People",
		"action_builder:field": "Relation",
		"name": "Friend"
	},
	{
		"action_builder:section": "Connection Between People",
		"action_builder:field": "Relation",
		"name": "Mentor",
		"action_builder:create_tag": true
	}]
  }]
}
		
					

				

Response

			 	 

200 OK

Content-Type: application/hal+json
Cache-Control: max-age=0, private, must-revalidate					 
					 
{
  "person": {
    "origin_system": "Action Builder",
    "identifiers": [
      "action_builder:42d941bd-03e4-486f-99be-b4b49bc0cdb7"
    ],
    "created_date": "2022-01-24T19:58:09.576Z",
    "modified_date": "2022-01-24T19:58:09.576Z",
    "action_builder:entity_type": "person",
    "given_name": "Sunny",
    "family_name": "Smith",
    "browser_url": "https://techworkersunited.actionbuilder.org/entity/view/20567/profile?campaignId=11415",
    "preferred_language": "en",
    "_links": {
      "self": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people/42d941bd-03e4-486f-99be-b4b49bc0cdb7"
      },
      "action_builder:campaign": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f"
      },
      "action_builder:entity_type": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/entity_types/786888a8-868f-4694-a3ba-1caf0688d0c0"
      },
      "action_builder:connections": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people/42d941bd-03e4-486f-99be-b4b49bc0cdb7/connections"
      },
      "osdi:taggings": {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/people/42d941bd-03e4-486f-99be-b4b49bc0cdb7/taggings"
      }
    }
  }
}
				 

			  