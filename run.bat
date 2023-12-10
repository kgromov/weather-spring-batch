set SPRING_DATA_MONGODB_URI=
set profile=%1

set projectDir=
cd %projectDir%
set currentDir=%cd%
if not %currentDir% == %projectDir% (
D:
)
mvn spring-boot:run "-Dspring-boot.run.arguments=--spring.profiles.active=%profile% -Duser.timezone=UTC"