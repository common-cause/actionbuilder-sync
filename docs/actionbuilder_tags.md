Tags

Tags are resources that contain information on your entities and connections. Tags correspond to info on the UI. Each tag belongs to a section and has a type and different responses.

Tags are grouped by sections, which can hold many fields. Fields can have a type, such as standard, number, date, address, note, or shift. Each of these different types will correspond to a different response in Taggings.

    Endpoints and URL Structures
    Field Names and Descriptions
    Links
    Related Resources
    Scenario: Retrieving a Collection of Tag Resources (GET)
    Scenario: Retrieving an Individual Tag Resource (GET)
    Scenario: Creating a New Tag Resource (POST)
    Scenario: PUT
    Scenario: Deleting a Tag Resource (DELETE)

Endpoints and URL Structures


https://[your-sub-domain].actionbuilder.org/api/rest/v1/campaigns/[campaign_id]/tags

					

Tag resources live exclusively at the above endpoint. The endpoint returns a collection of all the tags associated with your API key and specified campaign.

URL Structures:


https://[your-sub-domain].actionbuilder.org/api/rest/v1/campaigns/[campaign_id]/tags/[tag_id]

To address a specific tag, use the resource type name like:


https://techworkersunited.actionbuilder.org/api/v1/rest/campaigns/d91b4b2e-ae0e-4cd3-9ed7-d0ec501b0bc3/tags/e5b603ec-1ddc-45d3-ace2-6088006e2f39

					 

Back To Top ↑
Field Names and Descriptions

Tag Fields
Field Name 	Type 	Required on POST 	Description
origin_system 	string 		A human readable string identifying where this tag originated.
identifiers 	strings[] 		An array containing the read-only Action Builder identifier of this resource in the format [system name]:[id]. See the general concepts document for more information about identifiers.
created_date 	datetime 		The date and time the resource was created. System generated, not editable.
modified_date 	datetime 		The date and time the resource was last modified. System generated, not editable.
name 	string 	Yes 	The response name for this field.
action_builder:section 	string 	Yes 	The group this tag belongs to. Must match existing section on POST.
action_builder:field 	string 	Yes 	The name of the field, corresponds to field on the UI. Must match existing field on on POST.
action_builder:field_type 	string 		The data type for the field. One of 'standard', 'number', 'date', 'address', 'note', or 'shift'.
action_builder:locked 	boolean 		Indicates whether this field's responses are locked. True if organizers cannot add responses, false if they cannot.
action_builder:allow_multiple_responses 	boolean 		Indicates whether this field allows multiple responses. One of either true or false.
action_builder:postal_addresses 	postal_addresses 	Yes, if field_type is address 	The response for a address type field. Will only be present for these types of fields.

Postal Addresses Fields
Field Name 	Type 	Required on POST 	Description
postal_addresses.address_lines 	string[] 		An array of strings representing the tag’s street address.
postal_addresses.locality 	string 		A city or other local administrative area.
postal_addresses.region 	string 	Yes 	The state or region represented by the two digit abbreviation. Only supports US and Canada, leave blank for Great Britain.
postal_addresses.postal_code 	string 		The region specific postal code, such as a zip code.
postal_addresses.country 	string 	Yes 	The country code according to ISO 3166-1 Alpha-2. Only supports US, CA, or GB.
postal_addresses.source 	string 		The origin of this postal address. System generated, not editable.

Back To Top ↑
Links
Link Name 	Description
self 	A link to this individual tag resource.
action_builder:campaign 	A link to the campaign this entity type is in. Click here for campaigns documentation.
osdi:taggings 	A link to a collection of all tagging resources associated with this tag. Click here for taggings documentation.

Back To Top ↑
Related Resources

    Campaigns
    Taggings


Back To Top ↑
Scenario: Retrieving a collection of tag resources (GET)

Tag resources are sometimes presented as collections of tags. For example, calling the tags endpoint will return a collection of all the tags associated with that API key and specified campaign.

Request

						

GET https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags		

Header:
OSDI-API-Token: your_api_key_here					
						

					

Response

						

{
  "per_page": 25,
  "page": 1,
  "total_pages": 1,
  "_embedded": {
    "osdi:tags": [
      {
        "origin_system": "Action Builder",
        "identifiers": [
          "action_builder:68615557-fb83-4c08-b8f6-3e4b7b37f6e7"
        ],
        "created_date": "2022-02-18T16:53:22.172Z",
        "modified_date": "2022-02-18T16:53:22.172Z",
        "name": "Because we need protections",
        "action_builder:section": "Basic Info",
        "action_builder:field": "Why are you unionizing?",
        "action_builder:field_type": "notes",
        "action_builder:locked": false,
        "action_builder:allow_multiple_responses": true,
        "_links": {
          "self": {
            "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/68615557-fb83-4c08-b8f6-3e4b7b37f6e7"
          },
          "action_builder:campaign": {
            "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f"
          },
          "osdi:taggings": {
            "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/68615557-fb83-4c08-b8f6-3e4b7b37f6e7/taggings"
          }
        }
      },
      {
        "origin_system": "Action Builder",
        "identifiers": [
          "action_builder:588dbacf-0f20-47cf-ad7b-a17f6c39b00f"
        ],
        "created_date": "2022-02-18T16:38:20.215Z",
        "modified_date": "2022-02-18T16:38:20.215Z",
        "name": "1/15/2022",
        "action_builder:section": "Basic Info",
        "action_builder:field": "Hiring Date",
        "action_builder:field_type": "date",
        "action_builder:locked": false,
        "action_builder:allow_multiple_responses": false,
        "_links": {
          "self": {
            "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/588dbacf-0f20-47cf-ad7b-a17f6c39b00f"
          },
          "action_builder:campaign": {
            "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f"
          },
          "osdi:taggings": {
            "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/588dbacf-0f20-47cf-ad7b-a17f6c39b00f/taggings"
          }
        }
      },
      // truncated for brevity
    ]
  },
  "_links": {
    "self": {
      "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags"
    },
    "curies": [
      {
        "name": "osdi",
        "href": "https://actionbuilder.org/docs/v1/{rel}",
        "templated": true
      },
      {
        "name": "action_builder",
        "href": "https://actionbuilder.org/docs/v1/{rel}",
        "templated": true
      }
    ],
    "osdi:tags": [
      {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/68615557-fb83-4c08-b8f6-3e4b7b37f6e7"
      },
      {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/588dbacf-0f20-47cf-ad7b-a17f6c39b00f"
      },
      {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/3bf664da-0055-4009-ad2f-679aad4ecb2c"
      },
      {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/f3534b6f-83e3-4ebd-ab32-570d69402d24"
      },
      {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/63cff13b-e528-4521-9c8b-36d837a1cf05"
      },
      {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/16e3564c-4348-4261-be93-06bb43352493"
      },
      {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/0e51a7e5-321c-4d39-b216-31a57f04e5f4"
      },
      {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/b79e1d32-507b-4e7d-8cdd-7d7c9ccb3308"
      },
      {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/1cfd2a2f-f598-45f4-a977-11bc8fab6b10"
      },
      {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/689157fd-5e5b-403d-9913-c427a2e6c5bf"
      },
      {
        "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/5d79d3c0-5448-4ec7-b421-8ec06d09fb8a"
      }
    ]
  }
}		
						

					


Back To Top ↑
Scenario: Retrieving an individual tag resource (GET)

Calling an individual tag resource will return the tag directly, along with all associated fields and appropriate links to additional information about the tag.

Request

					

GET https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/689157fd-5e5b-403d-9913-c427a2e6c5bf

Header:
OSDI-API-Token: your_api_key_here						
					

				

Response

			 	 

200 OK

Content-Type: application/hal+json
Cache-Control: max-age=0, private, must-revalidate					 
					 
{
  "origin_system": "Action Builder",
  "identifiers": [
    "action_builder:689157fd-5e5b-403d-9913-c427a2e6c5bf"
  ],
  "created_date": "2021-02-03T21:04:36.237Z",
  "modified_date": "2021-02-03T21:04:36.237Z",
  "name": "She/Hers",
  "action_builder:section": "Basic Info",
  "action_builder:field": "Pronouns",
  "action_builder:field_type": "standard",
  "action_builder:locked": false,
  "action_builder:allow_multiple_responses": false,
  "_links": {
    "curies": [
      {
        "name": "osdi",
        "href": "https://actionbuilder.org/docs/v1/{rel}",
        "templated": true
      },
      {
        "name": "action_builder",
        "href": "https://actionbuilder.org/docs/v1/{rel}",
        "templated": true
      }
    ],
    "self": {
      "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/689157fd-5e5b-403d-9913-c427a2e6c5bf"
    },
    "action_builder:campaign": {
      "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f"
    },
    "osdi:taggings": {
      "href": "https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/689157fd-5e5b-403d-9913-c427a2e6c5bf/taggings"
    }
  }
}		 
				 

			  


Back To Top ↑
Scenario: Creating a new tag resource (POST)

Creating a new tag resource will return the tag directly, along with all associated fields and appropriate links to additional information about the tag. The values for action_builder:field and action_builder:section must match existing fields and sections in the UI.

Request

					

POST https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags

Header:
OSDI-API-Token: your_api_key_here

{
  "name": "1600 Pennsylvania Ave",
  "action_builder:field": "Meeting Place",
  "action_builder:section": "Basic Info",
  "action_builder:postal_addresses": [
   {
     "address_lines": [
       "1600 Pennsylvania Ave NW"
     ],
     "locality": "Washington",
     "region": "DC",
     "postal_code": "20500",
     "country": "US"
   }
  ]
}						
					

				

Response

			 	 

200 OK

Content-Type: application/hal+json
Cache-Control: max-age=0, private, must-revalidate					 
					 
{
  "origin_system": "Action Builder",
  "identifiers": [
    "action_builder:b34d2074-de72-498f-9704-4f3a97571594"
  ],
  "created_date": "2022-09-22T15:08:55.081Z",
  "modified_date": "2022-09-22T15:08:55.167Z",
  "name": "1600 Pennsylvania Ave",
  "action_builder:section": "Basic Info",
  "action_builder:field": "Meeting Place",
  "action_builder:field_type": "address",
  "action_builder:locked": false,
  "action_builder:allow_multiple_responses": false,
  "action_builder:postal_addresses": [
    {
      "address_lines": [
        "1600 Pennsylvania Ave NW"
      ],
      "locality": "Washington",
      "region": "DC",
      "postal_code": "20500",
      "country": "US",
      "source": "action_builder"
    }
  ],
  "_links": {
    "self": {
      "href": "https://campaigntest.weinteractdev.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/b34d2074-de72-498f-9704-4f3a97571594"
    },
    "action_builder:campaign": {
      "href": "https://campaigntest.weinteractdev.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f"
    },
    "osdi:taggings": {
      "href": "https://campaigntest.weinteractdev.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/b34d2074-de72-498f-9704-4f3a97571594/taggings"
    }
  }
}	 
				 

			  


Back To Top ↑
Scenario: PUT

Putting of tags is not allowed. Attempts will result in errors.

Back To Top ↑
Scenario: Deleting an individual tag resource (DELETE)

Deleting a tag resource will remove the tag from the campaign. All tagging data collected for this tag in the campaign will not be displayed in the UI or API.

Request

					

DELETE https://techworkersunited.actionbuilder.org/api/rest/v1/campaigns/84a684a7-2f5a-4359-bdb4-898ce4fbc88f/tags/689157fd-5e5b-403d-9913-c427a2e6c5bf

Header:
OSDI-API-Token: your_api_key_here						
					

				

Response

			 	 

200 OK

Content-Type: application/hal+json
Cache-Control: max-age=0, private, must-revalidate					 
					 
{
  "message": "Tag has been removed from the campaign"
}
				 

			  