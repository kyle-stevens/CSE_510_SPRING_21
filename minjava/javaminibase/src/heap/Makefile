JDKPATH = /usr/lib/jvm/java-11-openjdk-amd64/
LIBPATH = /mnt/c/Users/kyleb/Desktop/GITHUB_REPOS/CSE_510_SPRING_21/minjava/javaminibase/src
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

hf:Heapfile.java
	$(JAVAC) Heapfile.java

hp:HFPage.java
	$(JAVAC) HFPage.java

tuple:Tuple.java
	$(JAVAC) Tuple.java

scan:Scan.java
	$(JAVAC) Scan.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
