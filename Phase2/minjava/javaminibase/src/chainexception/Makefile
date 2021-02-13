JDKPATH = /usr/lib/jvm/java-11-openjdk-amd64
LIBPATH = /home/solas/minjava/javaminibasesrc
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
