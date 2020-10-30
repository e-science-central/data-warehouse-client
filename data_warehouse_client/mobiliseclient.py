#
# All client code in a single file for event data access
#
import json
import requests

#
# Subset of the e-SC datatypes necessary to fetch event data
# 
class EscEvent:
    def __init__(self):
        self.eventType = ""
        self.timestamp = 0
        self.metadata = {}
        self.data = {}

    def parseDict(self, dict):
        self.eventType = dict['eventType']
        self.timestamp = dict['timestamp']
        self.data = dict['data']
        self.metadata = dict['metadata']

# Study object
class EscStudyObject:
    def __init__(self):
        self.id = 0
        self.studyId = 0
        self.folderId = ''
        self.externalId = ''
        self.name = ''
        self.additionalProperties = {}       

    def parseDict(self, dict):
        self.id = dict['id']
        self.studyId = dict['studyId']
        self.folderId = dict['folderId']
        self.externalId = dict['externalId']
        self.name = dict['name']
        self.additionalProperties = dict['additionalProperties']

    def toDict(self):
        return {
            'id': self.id,
            'studyId': self.studyId,
            'folderId': self.folderId,
            'externalId': self.extrnalId,
            'name': self.name,
            'additionalProperties': self.additionalProperties
        }

# Person in a study
class EscPerson(EscStudyObject):
    def __init__(self):
        EscStudyObject.__init__(self)    
        self.objectType = "EscPerson"

# JWT Token object
class EscJWT:
    def __init__(self):
        self.token = ''
        self.id = ''
        self.expiryTimestamp = 0
        self.refreshToken = ''

    # Create from JSON
    def parseDict(self, dict):
        self.expiryTimestamp = dict['expiryTimestamp']
        self.token = dict['token']
        self.id = dict['id']
        self.refreshToken = dict['refreshToken']
        
    # Write to JSON
    def toDict(self):
        return {
            "expiryTimestamp": self.expiryTimestamp,
            "token": self.token,
            "id": self.id,
            "refreshToken": self.refreshToken
        }

# Project / Study object
class EscProject:
    def __init__(self):
        self.id = ''
        self.name = ''
        self.description = ''
        self.workflowFolderId = ''
        self.dataFolderId = ''
        self.creatorId = ''
        self.externalId = ''
        self.projectType = 'HEIRARCHICAL'  

    def parseDict(self, dict):
        self.id = dict['id']
        self.name = dict['name']
        self.description = dict['description']
        self.workflowFolderId = dict['workflowFolderId']
        self.dataFolderId = dict['dataFolderId']
        self.creatorId = dict['creatorId']
        self.externalId = dict['externalId']
        self.projectType = dict['projectType']

    def toDict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "workflowFolderId": self.workflowFolderId,
            "dataFolderId": self.dataFolderId,
            "creatorId": self.creatorId,
            "externalId": self.externalId,
            "projectType": self.projectType
        }

# Base class for ServerObjects
class EscObject:
    def __init__(self):
        self.id = ''
        self.name = ''
        self.description = ''
        self.creatorId = ''
        self.projectId = ''
        self.containerId = ''
        self.internalClassName = ''
        self.creationTime = 0

    def parseDict(self, dict):
        self.id = dict['id']
        self.name = dict['name']
        self.description = dict['description']
        self.creatorId = dict['creatorId']
        self.projectId = dict['projectId']
        self.containerId = dict['containerId']
        self.internalClassName = dict['internalClassName']
        self.creationTime = dict['creationTime']

    def toDict(self, dict):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "creatorId": self.creatorId,
            "projectId": self.projectId,
            "containerId:": self.containerId,
            "internalClassName": self.internalClassName,
            "creationTime": self.creationTime
        }
        
# Folder object
class EscFolder(EscObject):
    def __init__(self):
        EscObject.__init__(self)     

# Document object
class EscDocument(EscObject):
    def __init__(self):
        EscObject.__init__(self)
        self.currentVersionSize = 0
        self.currentVersionNumber = 0
        self.currentVersionHash = ''
        self.downloadPath = ''
        self.uploadPath = ''

    def parseDict(self, dict):
        self._EscObject__parseDict(dict)
        self.currentVersionSize = dict['currentVersionSize']
        self.currentVersionNumber = dict['currentVersionNumber']
        self.currentVersionHash = dict['currentVersionHash']
        self.downloadPath = dict['downloadPath']
        self.uploadPath = dict['uploadPath']

    def toDict(self):
        dict = self.__EscObject_toDict()
        dict['currentVersionSize'] = self.currentVersionSize
        dict['currentVersionNumber'] = self.currentVersionNumber
        dict['currentVersionHash'] = self.currentVersionHash
        dict['downloadPath'] = self.downloadPath
        dict['uploadPath'] = self.uploadPath

    
#
# Combined client object
#
class EscClient:
    def __init__(self, hostname, port, ssl):
        self.jwt = ""
        self.hostname = hostname
        self.port = port
        self.ssl = ssl

    # Create a url
    def __create_url(self, url):
        if self.ssl==True:
            return 'https://' + self.hostname + ':' + str(self.port) + url
        else:
            return 'http://' + self.hostname + ':' + str(self.port) + url


    # Create a form body that can be POSTed using a dict of name:value pairs
    def __create_form_body(self, body_dict):
        count = 0;
        body = '';
        for key in body_dict:
            if count > 0:
                body = body + '&'

            body = body + key + '=' + body_dict[key]
            count=count+1;

        return body

    # Create the request headers
    def __create_headers(self):
        return {
            'Authorization' : 'Bearer ' + self.jwt
        }
        
    # Send a Form using the POST method
    def __post_form_retrieve_json(self, url, form_data, send_auth):
        if send_auth==True:
            r = requests.post(self.__create_url(url), form_data, headers=self.__create_headers())
            return r.json()

        else:
            r = requests.post(self.__create_url(url), form_data)
            return r.json()

        
    # Delete a resource
    def __delete_resource(self, url):
        requests.delete(self.__create_url(url), headers=self.__create_headers())

    # Post text and get text back
    def __post_text_retrieve_text(self, url, text_data):
        headers = self.__create_headers()
        headers['content-type'] = 'text/plain'
        r = requests.post(self.__create_url(url), data=text_data.encode('utf-8'), headers=headers)
        return r.text

    def __post_json_retrieve_json(self, url, json_data):
        headers = self.__create_headers()
        headers['content-type'] = 'application/json'
        r = requests.post(self.__create_url(url), data=json_data, headers=headers)
        return r.json()

    def __retrieve_json(self, url):
        headers = self.__create_headers()
        r = requests.get(self.__create_url(url), headers=headers)
        return r.json()

    def __retrieve_text(self, url):
        headers = self.__create_headers()
        r = requests.get(self.__create_url(url), headers=headers)
        return r.text


    # =======================================================================================
    #
    # Implementation of standard e-SC client methods
    #
    # =======================================================================================

    #
    # Issue an access token using a username and password
    #
    def issueToken(self, username, password, label):
        auth_details = {
            "username": username,
            "password": password,
            "label": label
        }

        result = self._EscClient__post_form_retrieve_json("/api/public/rest/v1/tokens/issue", auth_details, False)        
        jwt = EscJWT()
        jwt.parseDict(result)
        return jwt

    #
    # Release an access token, which prevents its subsequent use
    #
    def releaseToken(self, id):
        self._EscClient__delete_resource(id)

    #
    # Check whether a JWT is still valid
    #
    def validateToken(self, token):
        return self._EscClient__post_text_retrieve_text("/api/public/rest/v1/tokens/validate", token)

    #
    # Returns a list of the projects that the authenticated user is permitted to view
    #
    def listProjects(self):
        jsonData = self._EscClient__retrieve_json("/api/public/rest/v1/storage/projects");
        results = {}
        for i in range(0, len(jsonData)):
            project = EscProject();
            project.parseDict(jsonData[i])
            results[i] = project

        return results

    #
    # Return a folder object given its database id
    #
    def getFolder(self, id):
        jsonData = self._EscClient__retrieve_json("/api/public/rest/v1/storage/folders/" + id)    
        folder = EscFolder();
        folder.parseDict(jsonData)
        return folder

    #
    # Access a person using their externally visible ID. i.e. the PatientID
    #
    def getPersonByExternalId(self, externalId):
        jsonData = self._EscClient__retrieve_json("/api/public/rest/v1/catalog/peoplebyexternalid/" + externalId)
        person = EscPerson()
        person.parseDict(jsonData)
        return person

    #
    # Access a study given its externally visible ID
    #
    def getProjectByStudyCode(self, studyCode):
        jsonData = self._EscClient__retrieve_json("/api/public/rest/v1/catalog/studiesbyexternalcode/" + studyCode)
        project = EscProject()
        project.parseDict(jsonData)
        return project

    #
    # Return the number of event objects contained in a study
    #
    def getEventCount(self, studyCode):
        return int(self._EscClient__retrieve_text("/api/public/rest/v1/catalog/studiesbyid/" + studyCode + "/allevents/count"))

    #
    # Get a set of events
    #
    def queryEventsFromStudy(self, studyCode, startIndex, pageSize):
        jsonData = self._EscClient__retrieve_json("/api/public/rest/v1/catalog/studiesbyid/" + studyCode + "/allevents/" + str(startIndex) + "/" + str(pageSize))
        results = {}
        for i in range(0, len(jsonData)):
            evt = EscEvent()
            evt.parseDict(json.loads(jsonData[i]))
            results[i] = evt

        return results

    #
    # Get the number of people in a study
    #
    def getNumberOfPeopleInStudy(self, projectId):
        return int(self._EscClient__retrieve_text("/api/public/rest/v1/catalog/studiesbyid/" + str(projectId) + "/people/count"))

    #
    # Get a set of people from a study
    #
    def getPeople(self, projectId, startIndex, count):
        jsonData = self._EscClient__retrieve_json("/api/public/rest/v1/catalog/studiesbyid/" + str(projectId) + "/people/list/" + str(startIndex) + "/" + str(count))
        results = {}
        for i in range(0, len(jsonData)):
            person = EscPerson()
            person.parseDict(jsonData[i])
            results[i] = person
        
        return results

                