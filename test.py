from mobiliseclient import *

hostname = "mobilised.di-projects.net"
port = 443
ssl = True

# Log onto the system and obtain a JWT
mc = EscClient(hostname, port, ssl)
#print('Issuing')
#jwt = tc.issueToken("username", "password", "Python Test")
# The actual token is a field of the returned jwt object
#token = jwt.token

# Use an existing JWT
token = 'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI1NSIsInJvbGUiOiJ1c2VyIiwiZXhwIjoxNjI2MzQ5ODU4LCJpYXQiOjE1OTQ4MTM4NTgsImp0aSI6IjJjOWZhZmU1NzMwZjZkZGMwMTczNTI1MDQwZmYzYjBhIn0.YLOQhgHrcKuGBiCLUEiDHNA74666-dDwgNSD6FHdD03UkW2tUoMwJAsgAaYKa6PBZfBy95vHB4XrPcXDhyfCtA'
mc.jwt = token

# Use this to validate a token that you already have
print('Validating')
print(mc.validateToken(token))

# Find a person by ID
person = mc.getPersonByExternalId('DSTST01')
print(person.externalId)

# Find a project by ID
project = mc.getProjectByStudyCode("BB001")

# Get all of the people in a study - N.B some methods use the study database id and not the external ID
personCount = mc.getNumberOfPeopleInStudy(project.id)
print("Number of people in study: " + str(personCount))
people = mc.getPeople(project.id, 0, personCount)
for i in range(0, len(people)):
    print(people[i].externalId)
    
dataFolder = mc.getFolder(project.dataFolderId)
print("Project data folder: " + dataFolder.name)

# Retrieve all of the events from the project in a big block
eventCount = mc.getEventCount(project.externalId)
print("Events in project: " + str(eventCount))
events = mc.queryEventsFromStudy(project.externalId, 0, eventCount)
for i in range(0, len(events)):
    evt = events[i]
    print("ID: " + evt.metadata['_id'] + ": schema: " + evt.metadata['_eventType'] + ": data: " + str(evt.data))

   
    
projects = mc.listProjects()
for i in range(0, len(projects)):
    p = projects[i]
    print(p.externalId)

# If you obtained you own token, release it here so that we don't get loads of tokens hanging around
#print('Releasing')
#mc.releaseToken(jwt.id)