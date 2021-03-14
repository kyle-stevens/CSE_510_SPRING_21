# modify the paths appropriately
JDKPATH = /usr/lib/jvm/java-11-openjdk-amd64
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

#running client.java

Client:Client.java
	$(JAVAC) Client.java

client: Client
	$(JAVA) ui.Client
	
	
JDKPATH = /usr/lib/jvm/java-11-openjdk-amd64
LIBPATH = /mnt/c/Users/kyleb/Desktop/GITHUB_REPOS/CSE_510_SPRING_21/minjava/javaminibase/src
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
	
