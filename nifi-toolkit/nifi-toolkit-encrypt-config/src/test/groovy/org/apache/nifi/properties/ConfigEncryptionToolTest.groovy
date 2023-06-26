/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.properties

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.util.console.TextDevice
import org.apache.nifi.util.console.TextDevices
import org.junit.After
import org.junit.AfterClass
import org.junit.Assume
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.contrib.java.lang.system.Assertion
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.contrib.java.lang.system.SystemErrRule
import org.junit.contrib.java.lang.system.SystemOutRule
import org.junit.jupiter.api.Assertions
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.xmlunit.builder.DiffBuilder
import org.xmlunit.diff.DefaultNodeMatcher
import org.xmlunit.diff.Diff
import org.xmlunit.diff.ElementSelectors

import javax.crypto.Cipher
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.security.KeyException

class ConfigEncryptionToolTest {
    private static final Logger logger = LoggerFactory.getLogger(ConfigEncryptionToolTest.class)

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog()

    @Rule
    public final SystemErrRule systemErrRule = new SystemErrRule().enableLog()

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    public static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128
    private static final String PASSWORD = "thisIsABadPassword"
    
    private static final SensitivePropertyProviderFactory DEFAULT_PROVIDER_FACTORY = StandardSensitivePropertyProviderFactory.withKey(KEY_HEX)

    // From ConfigEncryptionTool.deriveKeyFromPassword("thisIsABadPassword")
    private static
    final String PASSWORD_KEY_HEX_256 = "2C576A9585DB862F5ECBEE5B4FFFCCA14B18D8365968D7081651006507AD2BDE"
    private static final String PASSWORD_KEY_HEX_128 = "2C576A9585DB862F5ECBEE5B4FFFCCA1"

    private static
    final String PASSWORD_KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? PASSWORD_KEY_HEX_256 : PASSWORD_KEY_HEX_128

    // Known issue documented in NIFI-1465 and NIFI-1255 where the password cannot be > 16 characters without the JCE unlimited strength policies installed
    private static final String FLOW_PASSWORD_128 = "shortPassword"
    private static final String FLOW_PASSWORD_256 = "thisIsABadPassword"
    public static
    final String FLOW_PASSWORD = isUnlimitedStrengthCryptoAvailable() ? FLOW_PASSWORD_256 : FLOW_PASSWORD_128

    private static final int LIP_PASSWORD_LINE_COUNT = 3
    private static final int AUTHORIZERS_PASSWORD_LINE_COUNT = 3
    private static final int AUTHORIZERS_SECRET_LINE_COUNT = 1
    private final String PASSWORD_PROP_REGEX = "<property[^>]* name=\".* Password\""
    private final String SECRET_PROP_REGEX = "<property[^>]* name=\".* Secret\""

    private static final String WFXCTR = ConfigEncryptionTool.WRAPPED_FLOW_XML_CIPHER_TEXT_REGEX

    @BeforeClass
    static void setUpOnce() throws Exception {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS)

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        setupTmpDir()
    }

    @AfterClass
    static void tearDownOnce() throws Exception {
        File tmpDir = new File("target/tmp/")
        tmpDir.delete()
    }

    @After
    void tearDown() throws Exception {
        System.clearProperty(NiFiProperties.PROPERTIES_FILE_PATH)
    }

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    private static int getKeyLength(String keyHex = KEY_HEX) {
        keyHex?.size() * 4
    }

    /**
     * OS-agnostic method for setting file permissions. On POSIX-compliant systems, accurately sets the provided permissions. On Windows, sets the corresponding permissions for the file owner only.
     *
     * @param file the file to modify
     * @param permissions the desired permissions
     */
    private static void setFilePermissions(File file, List<PosixFilePermission> permissions = []) {
        if (SystemUtils.IS_OS_WINDOWS) {
            file?.setReadable(permissions.contains(PosixFilePermission.OWNER_READ))
            file?.setWritable(permissions.contains(PosixFilePermission.OWNER_WRITE))
            file?.setExecutable(permissions.contains(PosixFilePermission.OWNER_EXECUTE))
        } else {
            Files.setPosixFilePermissions(file?.toPath(), permissions as Set)
        }
    }

    /**
     * OS-agnostic method for getting file permissions. On POSIX-compliant systems, accurately gets the existing permissions. On Windows, gets the corresponding permissions for the file owner only.
     *
     * @param file the file to check
     * @return a Set of (String, PosixFilePermissions) containing the permissions
     */
    private static Set getFilePermissions(File file) {
        if (SystemUtils.IS_OS_WINDOWS) {
            return [file.canRead() ? "OWNER_READ" : "",
                    file.canWrite() ? "OWNER_WRITE" : "",
                    file.canExecute() ? "OWNER_EXECUTE" : ""].findAll { it } as Set
        } else {
            return Files.getPosixFilePermissions(file?.toPath())
        }
    }

    private static boolean isValidDate(String formattedDate) {
        // The serialization could have occurred > 1 second ago, causing a rolling date/time mismatch, so use regex
        // Format -- #Fri Aug 19 16:51:16 PDT 2016
        // Alternate format -- #Fri Aug 19 16:51:16 GMT-05:00 2016
        // \u0024 == '$' to avoid escaping
        String datePattern = /^#\w{3} \w{3} \d{2} \d{2}:\d{2}:\d{2} \w{2,5}([\-+]\d{2}:\d{2})? \d{4}\u0024/

        formattedDate =~ datePattern
    }

    private static File setupTmpDir(String tmpDirPath = "target/tmp/") {
        File tmpDir = new File(tmpDirPath)
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])
        tmpDir
    }

    @Test
    void testShouldPrintHelpMessage() {
        // Arrange
        def flags = ["-h", "--help"]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            Assertions.assertThrows(CommandLineParseException.class, () -> tool.parse([arg] as String[]))
        }
    }

    @Test
    void testShouldParseBootstrapConfArgument() {
        // Arrange
        def flags = ["-b", "--bootstrapConf"]
        String bootstrapPath = "src/test/resources/bootstrap.conf"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            tool.parse([arg, bootstrapPath, "-n", "nifi.properties"] as String[])
            logger.info("Parsed bootstrap.conf location: ${tool.bootstrapConfPath}")

            // Assert
            assert tool.bootstrapConfPath == bootstrapPath
        }
    }

    @Test
    void testShouldParseNiFiPropertiesArgument() {
        // Arrange
        def flags = ["-n", "--niFiProperties"]
        String niFiPropertiesPath = "src/test/resources/nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            tool.parse([arg, niFiPropertiesPath] as String[])
            logger.info("Parsed nifi.properties location: ${tool.niFiPropertiesPath}")

            // Assert
            assert tool.niFiPropertiesPath == niFiPropertiesPath
            assert tool.handlingNiFiProperties
        }
    }

    @Test
    void testShouldParseOutputNiFiPropertiesArgument() {
        // Arrange
        def flags = ["-o", "--outputNiFiProperties"]
        String niFiPropertiesPath = "src/test/resources/nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            tool.parse([arg, niFiPropertiesPath, "-n", niFiPropertiesPath] as String[])
            logger.info("Parsed output nifi.properties location: ${tool.outputNiFiPropertiesPath}")

            // Assert
            assert tool.outputNiFiPropertiesPath == niFiPropertiesPath
        }
    }

    @Test
    void testParseShouldWarnIfNiFiPropertiesWillBeOverwritten() {
        // Arrange
        String niFiPropertiesPath = "conf/nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse("-n ${niFiPropertiesPath} -o ${niFiPropertiesPath}".split(" ") as String[])
    }

    @Test
    void testShouldParseLoginIdentityProvidersArgument() {
        // Arrange
        def flags = ["-l", "--loginIdentityProviders"]
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers.xml"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            tool.parse([arg, loginIdentityProvidersPath] as String[])
            logger.info("Parsed login-identity-providers.xml location: ${tool.loginIdentityProvidersPath}")

            // Assert
            assert tool.loginIdentityProvidersPath == loginIdentityProvidersPath
            assert tool.handlingLoginIdentityProviders
        }
    }

    @Test
    void testShouldParseOutputLoginIdentityProvidersArgument() {
        // Arrange
        def flags = ["-i", "--outputLoginIdentityProviders"]
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers.xml"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            tool.parse([arg, loginIdentityProvidersPath, "-l", loginIdentityProvidersPath] as String[])
            logger.info("Parsed output login-identity-providers.xml location: ${tool.outputLoginIdentityProvidersPath}")

            // Assert
            assert tool.outputLoginIdentityProvidersPath == loginIdentityProvidersPath
        }
    }

    @Test
    void testParseShouldWarnIfLoginIdentityProvidersWillBeOverwritten() {
        // Arrange
        String loginIdentityProvidersPath = "conf/login-identity-providers.xml"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse("-l ${loginIdentityProvidersPath} -i ${loginIdentityProvidersPath}".split(" ") as String[])
    }

    @Test
    void testShouldParseAuthorizersArgument() {
        // Arrange
        def flags = ["-a", "--authorizers"]
        String authorizersPath = "src/test/resources/authorizers.xml"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            tool.parse([arg, authorizersPath] as String[])
            logger.info("Parsed authorizers.xml location: ${tool.authorizersPath}")

            // Assert
            assert tool.authorizersPath == authorizersPath
            assert tool.handlingAuthorizers
        }
    }

    @Test
    void testShouldParseOutputAuthorizersArgument() {
        // Arrange
        def flags = ["-u", "--outputAuthorizers"]
        String authorizersPath = "src/test/resources/authorizers.xml"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            final outputAuthorizersPath = authorizersPath.reverse()
            tool.parse([arg, outputAuthorizersPath, "-a", authorizersPath] as String[])
            logger.info("Parsed output authorizers.xml location: ${tool.outputAuthorizersPath}")

            // Assert
            assert tool.outputAuthorizersPath == outputAuthorizersPath
        }
    }

    @Test
    void testParseShouldWarnIfAuthorizersWillBeOverwritten() {
        // Arrange
        String authorizersPath = "conf/authorizers.xml"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse("-a ${authorizersPath} -u ${authorizersPath}".split(" ") as String[])
    }

    @Test
    void testShouldParseKeyArgument() {
        // Arrange
        def flags = ["-k", "--key"]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            tool.parse([arg, KEY_HEX, "-n", "nifi.properties"] as String[])
            logger.info("Parsed key: ${tool.keyHex}")

            // Assert
            assert tool.keyHex == KEY_HEX
        }
    }

    @Test
    void testParseShouldFailIfMigrationPasswordAndKeyBothProvided() {
        // Arrange
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        Assertions.assertThrows(CommandLineParseException.class, () -> tool.parse("-m -n nifi.properties -e oldKey -w oldPassword".split(" ") as String[]))
    }

    @Test
    void testParseShouldFailIfPropertiesAndProvidersMissing() {
        // Arrange
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        def invalidArgs = [
                "-v -m",
                "-v -s password",
                "-n",
                "-l",
                "-a",
                "-o output-nifi.properties -i output-login-identity-providers.xml -u output-authorizers.xml",
                "-f flow.xml.gz",
        ]

        // Act
        invalidArgs.each { String badArgs ->
            Assertions.assertThrows(CommandLineParseException.class, () -> tool.parse(badArgs.split(" ") as String[]))
        }
    }

    @Test
    void testParseShouldIgnoreMigrationKeyAndPasswordIfMigrationNotEnabled() {
        // Arrange
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        def argStrings = ["-e oldKey",
                          "-w oldPassword"]

        // Act
        argStrings.each { String argString ->
            argString += " -n any/path"

            Assertions.assertThrows(Exception.class, () -> tool.parse(argString.split(" ") as String[]))
        }
    }

    @Test
    void testShouldLoadNiFiProperties() {
        // Arrange
        String niFiPropertiesPath = "src/test/resources/nifi_with_sensitive_properties_unprotected.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", niFiPropertiesPath] as String[]

        String oldFilePath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)

        tool.parse(args)
        logger.info("Parsed nifi.properties location: ${tool.niFiPropertiesPath}")

        // Act
        NiFiProperties properties = tool.loadNiFiProperties()
        logger.info("Loaded NiFiProperties from ${tool.niFiPropertiesPath}")

        // Assert
        assert properties
        assert properties.size() > 0

        // The system variable was reset to the original value
        assert System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH) == oldFilePath
    }

    @Test
    void testShouldReadKeyFromConsole() {
        // Arrange
        List<String> keyValues = [
                "0123 4567",
                KEY_HEX,
                "   ${KEY_HEX}   ",
                "non-hex-chars",
        ]

        // Act
        keyValues.each { String key ->
            TextDevice mockConsoleDevice = TextDevices.streamDevice(new ByteArrayInputStream(key.bytes), new ByteArrayOutputStream())
            String readKey = ConfigEncryptionTool.readKeyFromConsole(mockConsoleDevice)
            logger.info("Read key: [${readKey}]")

            // Assert
            assert readKey == key
        }
    }

    @Test
    void testShouldReadPasswordFromConsole() {
        // Arrange
        List<String> passwords = [
                "0123 4567",
                PASSWORD,
                "   ${PASSWORD}   ",
                "non-hex-chars",
        ]

        // Act
        passwords.each { String pw ->
            logger.info("Using password: [${PASSWORD}]")
            TextDevice mockConsoleDevice = TextDevices.streamDevice(new ByteArrayInputStream(pw.bytes), new ByteArrayOutputStream())
            String readPassword = ConfigEncryptionTool.readPasswordFromConsole(mockConsoleDevice)
            logger.info("Read password: [${readPassword}]")

            // Assert
            assert readPassword == pw
        }
    }

    @Test
    void testShouldReadPasswordFromConsoleIfNoKeyPresent() {
        // Arrange
        def args = ["-n", "nifi.properties"] as String[]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.parse(args)
        logger.info("Using password flag: ${tool.usingPassword}")
        logger.info("Password: ${tool.password}")
        logger.info("Key hex:  ${tool.keyHex}")

        assert tool.usingPassword
        assert !tool.password
        assert !tool.keyHex

        TextDevice mockConsoleDevice = TextDevices.streamDevice(new ByteArrayInputStream(PASSWORD.bytes), new ByteArrayOutputStream())

        // Act
        String readKey = tool.getKey(mockConsoleDevice)
        logger.info("Read key: [${readKey}]")

        // Assert
        assert readKey == PASSWORD_KEY_HEX
    }

    @Test
    void testShouldReadKeyFromConsoleIfFlagProvided() {
        // Arrange
        def args = ["-r", "-n", "nifi.properties"] as String[]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.parse(args)
        logger.info("Using password flag: ${tool.usingPassword}")
        logger.info("Password: ${tool.password}")
        logger.info("Key hex:  ${tool.keyHex}")

        assert !tool.usingPassword
        assert !tool.password
        assert !tool.keyHex

        TextDevice mockConsoleDevice = TextDevices.streamDevice(new ByteArrayInputStream(KEY_HEX.bytes), new ByteArrayOutputStream())

        // Act
        String readKey = tool.getKey(mockConsoleDevice)
        logger.info("Read key: [${readKey}]")

        // Assert
        assert readKey == KEY_HEX
    }

    @Test
    void testShouldIgnoreRawKeyFlagIfKeyProvided() {
        // Arrange
        def args = ["-r", "-k", KEY_HEX, "-n", "nifi.properties"] as String[]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse(args)

        // Assert
        assert !tool.usingPassword
        assert !tool.password
        assert tool.keyHex == KEY_HEX
    }

    @Test
    void testShouldIgnoreRawKeyFlagIfPasswordProvided() {
        // Arrange
        def args = ["-r", "-p", PASSWORD, "-n", "nifi.properties"] as String[]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse(args)

        // Assert
        assert tool.usingPassword
        assert tool.password == PASSWORD
        assert !tool.keyHex
    }

    @Test
    void testShouldParseKey() {
        // Arrange
        Map<String, String> keyValues = [
                (KEY_HEX)                         : KEY_HEX,
                ("   ${KEY_HEX}   " as String)    : KEY_HEX,
                ("xxx${KEY_HEX}zzz" as String)    : KEY_HEX,
                ((["0123", "4567"] * 4).join("-")): "01234567" * 4,
                ((["89ab", "cdef"] * 4).join(" ")): "89ABCDEF" * 4,
                (KEY_HEX.toLowerCase())           : KEY_HEX,
                (KEY_HEX[0..<32])                 : KEY_HEX[0..<32],
        ]

        if (isUnlimitedStrengthCryptoAvailable()) {
            keyValues.put(KEY_HEX[0..<48], KEY_HEX[0..<48])
        }

        // Act
        keyValues.each { String key, final String EXPECTED_KEY ->
            logger.info("Reading key: [${key}]")
            String parsedKey = ConfigEncryptionTool.parseKey(key)
            logger.info("Parsed key:  [${parsedKey}]")

            // Assert
            assert parsedKey == EXPECTED_KEY
        }
    }

    @Test
    void testShouldActuallyDeriveKeyFromPassword() {
        // Arrange
        logger.info("Using password: [${PASSWORD}]")

        // Act
        String derivedKey = ConfigEncryptionTool.deriveKeyFromPassword(PASSWORD)
        logger.info("Derived key:  [${derivedKey}]")

        // Assert
        assert derivedKey.length() == (Cipher.getMaxAllowedKeyLength("AES") > 128 ? 64 : 32)
    }

    @Test
    void testDeriveKeyFromPasswordShouldTrimPassword() {
        // Arrange
        final String PASSWORD_SPACES = "   ${PASSWORD}   "

        def attemptedPasswords = [PASSWORD, PASSWORD_SPACES]

        // Act
        def derivedKeys = attemptedPasswords.collect { String password ->
            logger.info("Using password: [${password}]")
            String derivedKey = ConfigEncryptionTool.deriveKeyFromPassword(password)
            logger.info("Derived key:  [${derivedKey}]")
            derivedKey
        }

        // Assert
        assert attemptedPasswords.size() == 2
        assert derivedKeys.every { it == PASSWORD_KEY_HEX }
    }

    @Test
    void testDeriveKeyFromPasswordShouldThrowExceptionForInvalidPasswords() {
        // Arrange
        List<String> passwords = [
                (null),
                "",
                "      ",
                "shortpass",
                "shortwith    "
        ]

        // Act
        passwords.each { String password ->
            Assertions.assertThrows(KeyException.class, () -> ConfigEncryptionTool.deriveKeyFromPassword(password))
        }
    }

    @Test
    void testShouldHandleKeyAndPasswordFlag() {
        // Arrange
        def args = ["-k", KEY_HEX, "-p", PASSWORD, "-n", ""]
        logger.info("Using args: ${args}")

        // Act
        Assertions.assertThrows(CommandLineParseException.class, () -> new ConfigEncryptionTool().parse(args as String[]))
    }

    @Test
    void testShouldNotLoadMissingNiFiProperties() {
        // Arrange
        String niFiPropertiesPath = "src/test/resources/non_existent_nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", niFiPropertiesPath] as String[]

        String oldFilePath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)

        tool.parse(args)
        logger.info("Parsed nifi.properties location: ${tool.niFiPropertiesPath}")

        // Act
        Assertions.assertThrows(CommandLineParseException.class, () -> tool.loadNiFiProperties())
    }

    @Test
    void testLoadNiFiPropertiesShouldHandleReadFailure() {
        // Arrange
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS)

        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File workingFile = new File("target/tmp_nifi.properties")
        workingFile.delete()

        Files.copy(inputPropertiesFile.toPath(), workingFile.toPath())
        // Empty set of permissions
        setFilePermissions(workingFile, [])
        logger.info("Set POSIX permissions to ${getFilePermissions(workingFile)}")

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)

        Assertions.assertThrows(IOException.class, () -> tool.loadNiFiProperties())
        workingFile.deleteOnExit()
    }

    @Test
    void testShouldEncryptSensitiveProperties() {
        // Arrange
        String niFiPropertiesPath = "src/test/resources/nifi_with_sensitive_properties_unprotected.properties"
        String newPropertiesPath = "src/test/resources/tmp_encrypted_nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", niFiPropertiesPath, "-o", newPropertiesPath] as String[]

        tool.parse(args)
        logger.info("Parsed nifi.properties location: ${tool.niFiPropertiesPath}")

        tool.keyHex = KEY_HEX

        NiFiProperties plainNiFiProperties = tool.loadNiFiProperties()
        ProtectedNiFiProperties protectedWrapper = new ProtectedNiFiProperties(plainNiFiProperties)
        assert !protectedWrapper.hasProtectedKeys()

        // Act
        NiFiProperties encryptedProperties = tool.encryptSensitiveProperties(plainNiFiProperties)
        logger.info("Encrypted sensitive properties")

        // Assert
        ProtectedNiFiProperties protectedWrapperAroundEncrypted = new ProtectedNiFiProperties(encryptedProperties)
        assert protectedWrapperAroundEncrypted.hasProtectedKeys()

        // Ensure that all non-empty sensitive properties are marked as protected
        final Set<String> EXPECTED_PROTECTED_KEYS = protectedWrapperAroundEncrypted
                .getSensitivePropertyKeys().findAll { String k ->
            plainNiFiProperties.getProperty(k)
        } as Set<String>
        assert protectedWrapperAroundEncrypted.getProtectedPropertyKeys().keySet() == EXPECTED_PROTECTED_KEYS
    }

    @Test
    void testShouldUpdateBootstrapContentsWithKey() {
        // Arrange
        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.keyHex = KEY_HEX

        List<String> originalLines = [
                ConfigEncryptionTool.BOOTSTRAP_KEY_COMMENT,
                "${ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX}="
        ]

        // Act
        List<String> updatedLines = tool.updateBootstrapContentsWithKey(originalLines)
        logger.info("Updated bootstrap.conf lines: ${updatedLines}")

        // Assert
        assert updatedLines.size() == originalLines.size()
        assert updatedLines.first() == originalLines.first()
        assert updatedLines.last() == EXPECTED_KEY_LINE
    }

    @Test
    void testUpdateBootstrapContentsWithKeyShouldOverwriteExistingKey() {
        // Arrange
        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.keyHex = KEY_HEX

        List<String> originalLines = [
                ConfigEncryptionTool.BOOTSTRAP_KEY_COMMENT,
                "${ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX}=badKey"
        ]

        // Act
        List<String> updatedLines = tool.updateBootstrapContentsWithKey(originalLines)
        logger.info("Updated bootstrap.conf lines: ${updatedLines}")

        // Assert
        assert updatedLines.size() == originalLines.size()
        assert updatedLines.first() == originalLines.first()
        assert updatedLines.last() == EXPECTED_KEY_LINE
    }

    @Test
    void testShouldUpdateBootstrapContentsWithKeyAndComment() {
        // Arrange
        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.keyHex = KEY_HEX

        List<String> originalLines = [
                "${ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX}="
        ]

        List<String> updatedLines = tool.updateBootstrapContentsWithKey(originalLines.clone() as List<String>)

        assert updatedLines.size() == originalLines.size() + 1
        assert updatedLines.first() == ConfigEncryptionTool.BOOTSTRAP_KEY_COMMENT
        assert updatedLines.last() == EXPECTED_KEY_LINE
    }

    @Test
    void testUpdateBootstrapContentsWithKeyShouldAddLines() {
        // Arrange
        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.keyHex = KEY_HEX

        List<String> originalLines = []

        // Act
        List<String> updatedLines = tool.updateBootstrapContentsWithKey(originalLines.clone() as List<String>)
        logger.info("Updated bootstrap.conf lines: ${updatedLines}")

        // Assert
        assert updatedLines.size() == originalLines.size() + 3
        assert updatedLines.first() == "\n"
        assert updatedLines[1] == ConfigEncryptionTool.BOOTSTRAP_KEY_COMMENT
        assert updatedLines.last() == EXPECTED_KEY_LINE
    }

    @Test
    void testShouldWriteKeyToBootstrapConf() {
        // Arrange
        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_root_key.conf")
        File workingFile = new File("target/tmp_bootstrap.conf")
        workingFile.delete()

        Files.copy(emptyKeyFile.toPath(), workingFile.toPath())
        final List<String> originalLines = workingFile.readLines()
        String originalKeyLine = originalLines.find { it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX) }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-b", workingFile.path, "-k", KEY_HEX, "-n", "nifi.properties"]
        tool.parse(args)

        // Act
        tool.writeKeyToBootstrapConf()
        logger.info("Updated bootstrap.conf")

        // Assert
        final List<String> updatedLines = workingFile.readLines()
        String updatedLine = updatedLines.find { it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX) }
        logger.info("Updated key line: ${updatedLine}")

        assert updatedLine == EXPECTED_KEY_LINE
        assert originalLines.size() == updatedLines.size()

        workingFile.deleteOnExit()
    }

    @Test
    void testShouldEncryptNiFiPropertiesWithEmptyProtectionScheme() {
        String originalNiFiPropertiesPath = "src/test/resources/nifi_with_sensitive_properties_unprotected_and_empty_protection_schemes.properties"

        NiFiProperties plainProperties = NiFiPropertiesLoader.withKey(KEY_HEX).load(originalNiFiPropertiesPath)
        ProtectedNiFiProperties protectedWrapper = new ProtectedNiFiProperties(plainProperties)
        ConfigEncryptionTool tool = new ConfigEncryptionTool(keyHex: KEY_HEX)

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)
        int protectedPropertyCount = protectedWrapper.getProtectedPropertyKeys().size()
        assert protectedPropertyCount < protectedWrapper.getSensitivePropertyKeys().size()

        NiFiProperties encryptedProperties = tool.encryptSensitiveProperties(plainProperties)

        ProtectedNiFiProperties encryptedWrapper = new ProtectedNiFiProperties(encryptedProperties)

        assert encryptedWrapper.getProtectedPropertyKeys().size() == encryptedWrapper.getSensitivePropertyKeys().findAll {
            encryptedWrapper.getProperty(it)
        }.size()
    }

    @Test
    void testShouldSerializeNiFiProperties() {
        // Arrange
        Assume.assumeTrue("Test only runs on *nix because Windows line endings are different", !SystemUtils.IS_OS_WINDOWS)

        Properties rawProperties = [key: "value", key2: "value2"] as Properties
        NiFiProperties properties = new NiFiProperties(rawProperties)
        logger.info("Loaded ${properties.size()} properties")

        // Act
        List<String> lines = ConfigEncryptionTool.serializeNiFiProperties(properties)
        logger.info("Serialized NiFiProperties to ${lines.size()} lines")
        logger.info("\n" + lines.join("\n"))

        // Assert

        // One extra line for the date
        assert lines.size() == properties.size() + 1

        rawProperties.keySet().every { String key ->
            assert lines.contains("${key}=${properties.getProperty(key)}".toString())
        }
    }

    @Test
    void testFirstLineOfSerializedPropertiesShouldBeLocalizedDateTime() {
        // Arrange
        Assume.assumeTrue("Test only runs on *nix because Windows line endings are different", !SystemUtils.IS_OS_WINDOWS)

        Properties rawProperties = [key: "value", key2: "value2"] as Properties
        NiFiProperties properties = new NiFiProperties(rawProperties)
        logger.info("Loaded ${properties.size()} properties")

        def currentTimeZone = TimeZone.default
        logger.info("Current time zone: ${currentTimeZone.displayName} (${currentTimeZone.ID})")

        // Configure different time zones
        def timeZones = TimeZone.availableIDs as List<String>

        // Act
        timeZones.each { String tz ->
            TimeZone.setDefault(TimeZone.getTimeZone(tz))

            String formattedDate = ConfigEncryptionTool.serializeNiFiProperties(properties).first()
            logger.info("First line date: ${formattedDate}")

            // Assert
            assert isValidDate(formattedDate)
        }

        // Restore current time zone
        TimeZone.setDefault(currentTimeZone)
        logger.info("Reset current time zone to ${currentTimeZone.displayName} (${currentTimeZone.ID})")
    }

    @Test
    void testShouldSerializeNiFiPropertiesAndPreserveFormatWithExistingProtectionSchemes() {
        String originalNiFiPropertiesPath = "src/test/resources/nifi_with_few_sensitive_properties_protected_aes.properties"

        File originalFile = new File(originalNiFiPropertiesPath)
        List<String> originalLines = originalFile.readLines()

        ProtectedNiFiProperties protectedProperties = NiFiPropertiesLoader.withKey(KEY_HEX).loadProtectedProperties(new File(originalNiFiPropertiesPath))
        int originalProtectedPropertyCount = protectedProperties.getProtectedPropertyKeys().size()

        protectedProperties.addSensitivePropertyProvider(DEFAULT_PROVIDER_FACTORY.getProvider(ConfigEncryptionTool.DEFAULT_PROTECTION_SCHEME))
        NiFiProperties encryptedProperties = protectedProperties.getApplicationProperties()
        int protectedPropertyCount = ProtectedNiFiProperties.countProtectedProperties(encryptedProperties)

        int protectedCountChange = protectedPropertyCount - originalProtectedPropertyCount
        List<String> lines = ConfigEncryptionTool.serializeNiFiPropertiesAndPreserveFormat(protectedProperties, originalFile)

        assert lines.size() == originalLines.size() + protectedCountChange

        protectedProperties.getPropertyKeys().every { String key ->
            assert lines.contains("${key}=${protectedProperties.getProperty(key)}".toString())
        }
    }

    @Test
    void testShouldWriteNiFiProperties() {
        // Arrange
        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File workingFile = new File("target/tmp_nifi.properties")
        workingFile.delete()

        final List<String> originalLines = inputPropertiesFile.readLines()

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", inputPropertiesFile.path, "-o", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)
        NiFiProperties niFiProperties = tool.loadNiFiProperties()
        tool.@niFiProperties = niFiProperties
        logger.info("Loaded ${niFiProperties.size()} properties from ${inputPropertiesFile.path}")

        // Act
        tool.writeNiFiProperties()
        logger.info("Wrote to ${workingFile.path}")

        // Assert
        final List<String> updatedLines = workingFile.readLines()
        niFiProperties.getPropertyKeys().every { String key ->
            assert updatedLines.contains("${key}=${niFiProperties.getProperty(key)}".toString())
        }

        if (originalLines.size() != updatedLines.size()) {
            // In situations where the original nifi.properties did not have a protection scheme for nifi.sensitive.props.key, it is added automatically now
            def differentLines = updatedLines - originalLines
            assert differentLines.size() == 1
            assert differentLines.first() == "nifi.sensitive.props.key.protected="
        } else {
            assert originalLines == updatedLines
        }

        logger.info("Updated nifi.properties:")
        logger.info("\n" * 2 + updatedLines.join("\n"))

        workingFile.deleteOnExit()
    }

    @Test
    void testShouldWriteNiFiPropertiesInSameLocation() {
        // Arrange
        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File workingFile = new File("target/tmp_nifi.properties")
        workingFile.delete()
        Files.copy(inputPropertiesFile.toPath(), workingFile.toPath())

        final List<String> originalLines = inputPropertiesFile.readLines()

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)
        NiFiProperties niFiProperties = tool.loadNiFiProperties()
        tool.@niFiProperties = niFiProperties
        logger.info("Loaded ${niFiProperties.size()} properties from ${workingFile.path}")

        // Act
        tool.writeNiFiProperties()
        logger.info("Wrote to ${workingFile.path}")

        // Assert
        final List<String> updatedLines = workingFile.readLines()
        niFiProperties.getPropertyKeys().every { String key ->
            assert updatedLines.contains("${key}=${niFiProperties.getProperty(key)}".toString())
        }

        if (originalLines.size() != updatedLines.size()) {
            // In situations where the original nifi.properties did not have a protection scheme for nifi.sensitive.props.key, it is added automatically now
            def differentLines = updatedLines - originalLines
            assert differentLines.size() == 1
            assert differentLines.first() == "nifi.sensitive.props.key.protected="
        } else {
            assert originalLines == updatedLines
        }

        workingFile.deleteOnExit()
    }

    @Test
    void testWriteNiFiPropertiesShouldHandleWriteFailureWhenFileExists() {
        // Arrange
        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File workingFile = new File("target/tmp_nifi.properties")
        workingFile.delete()

        Files.copy(inputPropertiesFile.toPath(), workingFile.toPath())
        // Read-only set of permissions
        setFilePermissions(workingFile, [PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ])
        logger.info("Set POSIX permissions to ${getFilePermissions(workingFile)}")

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", inputPropertiesFile.path, "-o", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)
        NiFiProperties niFiProperties = tool.loadNiFiProperties()
        tool.@niFiProperties = niFiProperties
        logger.info("Loaded ${niFiProperties.size()} properties from ${inputPropertiesFile.path}")

        // Act
        Assertions.assertThrows(IOException.class, () -> tool.writeNiFiProperties())
        workingFile.deleteOnExit()
    }

    @Test
    void testWriteNiFiPropertiesShouldHandleWriteFailureWhenFileDoesNotExist() {
        // Arrange
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS)

        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        File workingFile = new File("target/tmp/tmp_nifi.properties")
        workingFile.delete()

        // Read-only set of permissions
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ])
        logger.info("Set POSIX permissions to ${getFilePermissions(tmpDir)}")

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", inputPropertiesFile.path, "-o", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)
        NiFiProperties niFiProperties = tool.loadNiFiProperties()
        tool.@niFiProperties = niFiProperties
        logger.info("Loaded ${niFiProperties.size()} properties from ${inputPropertiesFile.path}")

        // Act
        Assertions.assertThrows(IOException.class, () -> tool.writeNiFiProperties())
        workingFile.deleteOnExit()
        setupTmpDir()
    }

    @Test
    void testShouldPerformFullOperation() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_root_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File outputPropertiesFile = new File("target/tmp/tmp_nifi.properties")
        outputPropertiesFile.delete()

        NiFiProperties inputProperties = new NiFiPropertiesLoader().load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        String[] args = ["-n", inputPropertiesFile.path, "-b", bootstrapFile.path, "-o", outputPropertiesFile.path, "-k", KEY_HEX]

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final List<String> updatedPropertiesLines = outputPropertiesFile.readLines()
                logger.info("Updated nifi.properties:")
                logger.info("\n" * 2 + updatedPropertiesLines.join("\n"))

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                NiFiProperties updatedProperties = new NiFiPropertiesLoader().loadProtectedProperties(outputPropertiesFile)
                assert updatedProperties.size() >= inputProperties.size()
                originalSensitiveValues.every { String key, String originalValue ->
                    assert updatedProperties.getProperty(key) != originalValue
                }

                // Check that the new NiFiProperties instance matches the output file (values still encrypted)
                updatedProperties.getPropertyKeys().every { String key ->
                    assert updatedPropertiesLines.contains("${key}=${updatedProperties.getProperty(key)}".toString())
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputPropertiesFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        ConfigEncryptionTool.main(args)
    }

    @Test
    void testShouldPerformFullOperationWithPassword() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_root_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX

        final String EXPECTED_KEY_HEX = PASSWORD_KEY_HEX
        logger.info("Derived key from password [${PASSWORD}]: ${EXPECTED_KEY_HEX}")

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + EXPECTED_KEY_HEX

        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File outputPropertiesFile = new File("target/tmp/tmp_nifi.properties")
        outputPropertiesFile.delete()

        NiFiProperties inputProperties = new NiFiPropertiesLoader().load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        String[] args = ["-n", inputPropertiesFile.path, "-b", bootstrapFile.path, "-o", outputPropertiesFile.path, "-p", PASSWORD]

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final List<String> updatedPropertiesLines = outputPropertiesFile.readLines()
                logger.info("Updated nifi.properties:")
                logger.info("\n" * 2 + updatedPropertiesLines.join("\n"))

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                NiFiProperties updatedProperties = new NiFiPropertiesLoader().loadProtectedProperties(outputPropertiesFile)
                assert updatedProperties.size() >= inputProperties.size()
                originalSensitiveValues.every { String key, String originalValue ->
                    assert updatedProperties.getProperty(key) != originalValue
                }

                // Check that the new NiFiProperties instance matches the output file (values still encrypted)
                updatedProperties.getPropertyKeys().every { String key ->
                    assert updatedPropertiesLines.contains("${key}=${updatedProperties.getProperty(key)}".toString())
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputPropertiesFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        ConfigEncryptionTool.main(args)
    }

    @Test
    void testShouldPerformFullOperationMultipleTimes() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_root_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX

        final String EXPECTED_KEY_HEX = PASSWORD_KEY_HEX
        logger.info("Derived key from password [${PASSWORD}]: ${EXPECTED_KEY_HEX}")

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + EXPECTED_KEY_HEX

        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File outputPropertiesFile = new File("target/tmp/tmp_nifi.properties")
        outputPropertiesFile.delete()

        NiFiProperties inputProperties = new NiFiPropertiesLoader().load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        String[] args = ["-n", inputPropertiesFile.path, "-b", bootstrapFile.path, "-o", outputPropertiesFile.path, "-p", PASSWORD, "-v"]

        Assertions.assertThrows(Exception.class, () -> ConfigEncryptionTool.main(args))

        // Act
        args = ["-n", outputPropertiesFile.path, "-b", bootstrapFile.path, "-p", PASSWORD, "-v"]

        // Add a new property to be encrypted
        outputPropertiesFile.text = outputPropertiesFile.text.replace("nifi.sensitive.props.additional.keys=", "nifi.sensitive.props.additional.keys=nifi.ui.banner.text")

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final List<String> updatedPropertiesLines = outputPropertiesFile.readLines()
                logger.info("Updated nifi.properties:")
                logger.info("\n" * 2 + updatedPropertiesLines.join("\n"))

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                NiFiProperties updatedProperties = new NiFiPropertiesLoader().loadProtectedProperties(outputPropertiesFile)
                assert updatedProperties.size() >= inputProperties.size()
                originalSensitiveValues.every { String key, String originalValue ->
                    assert updatedProperties.getProperty(key) != originalValue
                }

                // Check that the new NiFiProperties instance matches the output file (values still encrypted)
                updatedProperties.getPropertyKeys().every { String key ->
                    assert updatedPropertiesLines.contains("${key}=${updatedProperties.getProperty(key)}".toString())
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputPropertiesFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        ConfigEncryptionTool.main(args)
    }

    /**
     * Helper method to execute key migration test for varying combinations of old/new key/password.
     *
     * @param scenario a human-readable description of the test scenario
     * @param scenarioArgs a list of the arguments specific to this scenario to be passed to the tool
     * @param oldPassword the original password
     * @param newPassword the new password
     * @param oldKeyHex the original key hex (if present, original password is ignored; if not, this is derived)
     * @param newKeyHex the new key hex (if present, new password is ignored; if not, this is derived)
     */
    private void performKeyMigration(String scenario, List scenarioArgs, String oldPassword = PASSWORD, String newPassword = PASSWORD.reverse(), String oldKeyHex = "", String newKeyHex = "") {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        // Initial set up
        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])

        String bootstrapPath = isUnlimitedStrengthCryptoAvailable() ? "src/test/resources/bootstrap_with_root_key_password.conf" :
                "src/test/resources/bootstrap_with_root_key_password_128.conf"
        File originalKeyFile = new File(bootstrapPath)
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(originalKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }

        // Perform necessary key derivations
        if (!oldKeyHex) {
            oldKeyHex = ConfigEncryptionTool.deriveKeyFromPassword(oldPassword)
            logger.info("Original key derived from password [${oldPassword}]: \t${oldKeyHex}")
        } else {
            logger.info("Original key provided directly: \t${oldKeyHex}")
        }

        if (!newKeyHex) {
            newKeyHex = ConfigEncryptionTool.deriveKeyFromPassword(newPassword)
            logger.info("Migration key derived from password [${newPassword}]: \t${newKeyHex}")
        } else {
            logger.info("Migration key provided directly: \t${newKeyHex}")
        }

        // Reset the source bootstrap.conf file to use the old key (may be derived from old password)
        final String PASSWORD_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + oldKeyHex
        assert originalKeyLine == PASSWORD_KEY_LINE

        String inputPropertiesPath = isUnlimitedStrengthCryptoAvailable() ?
                "src/test/resources/nifi_with_sensitive_properties_protected_aes_password.properties" :
                "src/test/resources/nifi_with_sensitive_properties_protected_aes_password_128.properties"
        File inputPropertiesFile = new File(inputPropertiesPath)
        File outputPropertiesFile = new File("target/tmp/tmp_nifi.properties")
        outputPropertiesFile.delete()

        // Log original sensitive properties (encrypted with first key)
        NiFiProperties inputProperties = NiFiPropertiesLoader.withKey(oldKeyHex).load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        final String EXPECTED_NEW_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + newKeyHex

        // Act
        String[] args = ["-n", inputPropertiesFile.path,
                         "-b", bootstrapFile.path,
                         "-o", outputPropertiesFile.path,
                         "-m",
                         "-v"]

        List<String> localArgs = args + scenarioArgs
        logger.info("Running [${scenario}] with args: ${localArgs}")

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                assert outputPropertiesFile.exists()
                final List<String> updatedPropertiesLines = outputPropertiesFile.readLines()
                logger.info("Updated nifi.properties:")
                logger.info("\n" * 2 + updatedPropertiesLines.join("\n"))

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was re-encrypted)
                NiFiProperties updatedProperties = new NiFiPropertiesLoader().loadProtectedProperties(outputPropertiesFile)
                assert updatedProperties.size() >= inputProperties.size()
                originalSensitiveValues.every { String key, String originalValue ->
                    assert updatedProperties.getProperty(key) != originalValue
                }

                // Check that the new NiFiProperties instance matches the output file (values still encrypted)
                updatedProperties.getPropertyKeys().every { String key ->
                    assert updatedPropertiesLines.contains("${key}=${updatedProperties.getProperty(key)}".toString())
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_NEW_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputPropertiesFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        ConfigEncryptionTool.main(localArgs as String[])
    }

    /**
     * Ideally all of the combination tests would be a single test with iterative argument lists, but due to the System.exit(), it can only be captured once per test.
     */
    @Test
    void testShouldMigrateFromPasswordToPassword() {
        String scenario = "password to password"
        def args = ["-w", PASSWORD, "-p", PASSWORD.reverse()]

        performKeyMigration(scenario, args, PASSWORD, PASSWORD.reverse())
    }

    @Test
    void testShouldMigrateFromPasswordToKey() {
        String scenario = "password to key"
        def args = ["-w", PASSWORD, "-k", KEY_HEX]

        performKeyMigration(scenario, args, PASSWORD, "", "", KEY_HEX)
    }

    @Test
    void testShouldMigrateFromKeyToPassword() {
        String scenario = "key to password"
        def args = ["-e", PASSWORD_KEY_HEX, "-p", PASSWORD.reverse()]

        performKeyMigration(scenario, args, "", PASSWORD.reverse(), PASSWORD_KEY_HEX, "")
    }

    @Test
    void testShouldMigrateFromKeyToKey() {
        String scenario = "key to key"
        def args = ["-e", PASSWORD_KEY_HEX, "-k", KEY_HEX]

        performKeyMigration(scenario, args, "", "", PASSWORD_KEY_HEX, KEY_HEX)
    }

    @Test
    void testShouldDecryptLoginIdentityProviders() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-populated-encrypted.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Sanity check for decryption
        String propertyName = "Manager Password"
        String cipherText = "q4r7WIgN0MaxdAKM||SGgdCTPGSFEcuH4RraMYEdeyVbOx93abdWTVSWvh1w+klA"
        String EXPECTED_PASSWORD = "thisIsABadPassword"
        final SensitivePropertyProvider spp = StandardSensitivePropertyProviderFactory.withKey(KEY_HEX_128).getProvider(tool.protectionScheme)
        assert spp.unprotect(cipherText, ldapPropertyContext(propertyName)) == EXPECTED_PASSWORD

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def decryptedLines = tool.decryptLoginIdentityProviders(lines.join("\n")).split("\n")
        logger.info("Decrypted lines: \n${decryptedLines.join("\n")}")

        // Assert
        def passwordLines = decryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == LIP_PASSWORD_LINE_COUNT
        assert passwordLines.every { it =~ ">thisIsABadPassword<" }
        // Some lines were not encrypted originally so the encryption attribute would not have been updated
        assert passwordLines.any { it =~ "encryption=\"none\"" }
    }

    @Test
    void testShouldDecryptLoginIdentityProvidersWithMultilineElements() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-populated-encrypted-multiline.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def decryptedLines = tool.decryptLoginIdentityProviders(lines.join("\n")).split("\n")
        logger.info("Decrypted lines: \n${decryptedLines.join("\n")}")

        // Assert
        def passwordLines = decryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == LIP_PASSWORD_LINE_COUNT
        assert passwordLines.every { it =~ ">thisIsABadPassword<" }
        // Some lines were not encrypted originally so the encryption attribute would not have been updated
        assert passwordLines.any { it =~ "encryption=\"none\"" }
    }

    @Test
    void testShouldDecryptLoginIdentityProvidersWithMultipleElementsPerLine() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-populated-encrypted-multiple-per-line.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def decryptedLines = tool.decryptLoginIdentityProviders(lines.join("\n")).split("\n")
        logger.info("Decrypted lines: \n${decryptedLines.join("\n")}")

        // Assert
        def passwordLines = decryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == LIP_PASSWORD_LINE_COUNT
        assert passwordLines.every { it =~ ">thisIsABadPassword<" }
        // Some lines were not encrypted originally so the encryption attribute would not have been updated
        assert passwordLines.any { it =~ "encryption=\"none\"" }
    }


    @Test
    void testDecryptLoginIdentityProvidersShouldHandleCommentedElements() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-commented.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def decryptedLines = tool.decryptLoginIdentityProviders(lines.join("\n")).split("\n")
        logger.info("Decrypted lines: \n${decryptedLines.join("\n")}")

        // Assert

        // If no encrypted properties are found, the original input text is just returned (comments and formatting in tact)
        assert decryptedLines == lines
    }

    private static ProtectedPropertyContext ldapPropertyContext(final String propertyName) {
        ProtectedPropertyContext.contextFor("ldap", propertyName)
    }

    private static ProtectedPropertyContext nifiPropertiesContext(final String propertyName) {
        ProtectedPropertyContext.defaultContext(propertyName)
    }

    @Test
    void testShouldEncryptLoginIdentityProviders() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-populated.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        String encryptionScheme = "encryption=\"aes/gcm/${getKeyLength(KEY_HEX)}\""

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)

        // Act
        def encryptedLines = tool.encryptLoginIdentityProviders(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert
        def passwordLines = encryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == LIP_PASSWORD_LINE_COUNT
        assert passwordLines.every { !it.contains(">thisIsABadPassword<") }
        assert passwordLines.every { it.contains(encryptionScheme) }
        passwordLines.each {
            String ct = (it =~ ">(.*)</property>")[0][1]
            String propertyName = (it =~ 'name="(.*)"')[0][1]
            logger.info("Cipher text: ${ct}")
            assert spp.unprotect(ct, ldapPropertyContext(propertyName)) == PASSWORD
        }
    }

    @Test
    void testShouldEncryptLoginIdentityProvidersWithEmptySensitiveElements() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-populated-empty.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        String encryptionScheme = "encryption=\"aes/gcm/${getKeyLength(KEY_HEX)}\""

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)

        // Act
        def encryptedLines = tool.encryptLoginIdentityProviders(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert
        def passwordLines = encryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == LIP_PASSWORD_LINE_COUNT
        def populatedPasswordLines = passwordLines.findAll { it =~ />.+</ }
        assert populatedPasswordLines.every { !it.contains(">thisIsABadPassword<") }
        assert populatedPasswordLines.every { it.contains(encryptionScheme) }
        populatedPasswordLines.each {
            String ct = (it =~ ">(.*)</property>")[0][1]
            String propertyName = (it =~ 'name="(.*)"')[0][1]
            logger.info("Cipher text: ${ct}")
            assert spp.unprotect(ct, ldapPropertyContext(propertyName)) == PASSWORD
        }
    }

    @Test
    void testShouldEncryptLoginIdentityProvidersWithMultilineElements() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-populated-multiline.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        String encryptionScheme = "encryption=\"aes/gcm/${getKeyLength(KEY_HEX)}\""

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)

        // Act
        def encryptedLines = tool.encryptLoginIdentityProviders(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert
        def passwordLines = encryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == LIP_PASSWORD_LINE_COUNT
        assert passwordLines.every { !it.contains(">thisIsABadPassword<") }
        assert passwordLines.every { it.contains(encryptionScheme) }
        passwordLines.each {
            String ct = (it =~ ">(.*)</property>")[0][1]
            String propertyName = (it =~ 'name="(.*)"')[0][1]
            logger.info("Cipher text: ${ct}")
            assert spp.unprotect(ct, ldapPropertyContext(propertyName)) == PASSWORD
        }
    }

    @Test
    void testShouldEncryptLoginIdentityProvidersWithMultipleElementsPerLine() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-populated-multiple-per-line.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        String encryptionScheme = "encryption=\"aes/gcm/${getKeyLength(KEY_HEX)}\""

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)

        // Act
        def encryptedLines = tool.encryptLoginIdentityProviders(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert
        def passwordLines = encryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == LIP_PASSWORD_LINE_COUNT
        assert passwordLines.every { !it.contains(">thisIsABadPassword<") }
        assert passwordLines.every { it.contains(encryptionScheme) }
        passwordLines.each {
            String ct = (it =~ ">(.*)</property>")[0][1]
            String propertyName = (it =~ 'name="(.*)"')[0][1]
            logger.info("Cipher text: ${ct}")
            assert spp.unprotect(ct, ldapPropertyContext(propertyName)) == PASSWORD
        }
    }

    @Test
    void testShouldEncryptLoginIdentityProvidersWithRenamedProvider() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-populated-renamed.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        String encryptionScheme = "encryption=\"aes/gcm/${getKeyLength(KEY_HEX)}\""

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")
        assert lines.findAll { it =~ "ldap-provider" }.empty

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)

        // Act
        def encryptedLines = tool.encryptLoginIdentityProviders(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert
        def passwordLines = encryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == LIP_PASSWORD_LINE_COUNT
        def populatedPasswordLines = passwordLines.findAll { it =~ />.+</ }
        assert populatedPasswordLines.every { !it.contains(">thisIsABadPassword<") }
        assert populatedPasswordLines.every { it.contains(encryptionScheme) }
        populatedPasswordLines.each {
            String ct = (it =~ ">(.*)</property>")[0][1]
            String propertyName = (it =~ 'name="(.*)"')[0][1]
            logger.info("Cipher text: ${ct}")
            assert spp.unprotect(ct, ldapPropertyContext(propertyName)) == PASSWORD
        }
    }

    @Test
    void testEncryptLoginIdentityProvidersShouldHandleCommentedElements() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-commented.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def encryptedLines = tool.encryptLoginIdentityProviders(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert

        // If no sensitive properties are found, the original input text is just returned (comments and formatting in tact)
        assert encryptedLines == lines
    }

    @Test
    void testSerializeLoginIdentityProvidersAndPreserveFormatShouldRespectComments() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-populated.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        String plainXml = workingFile.text
        String encryptedXml = tool.encryptLoginIdentityProviders(plainXml, KEY_HEX)
        logger.info("Encrypted XML: \n${encryptedXml}")

        // Act
        def serializedLines = tool.serializeLoginIdentityProvidersAndPreserveFormat(encryptedXml, workingFile)
        logger.info("Serialized lines: \n${serializedLines.join("\n")}")

        // Assert

        // Some empty lines will be removed
        def trimmedLines = lines.collect { it.trim() }.findAll { it }
        def trimmedSerializedLines = serializedLines.collect { it.trim() }.findAll { it }
        assert trimmedLines.size() == trimmedSerializedLines.size()

        // Ensure the replacement actually occurred
        assert trimmedSerializedLines.findAll { it =~ "encryption=" }.size() == LIP_PASSWORD_LINE_COUNT
    }

    @Test
    void testSerializeLoginIdentityProvidersAndPreserveFormatShouldHandleRenamedProvider() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-populated-renamed.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")
        assert lines.findAll { it =~ "ldap-provider" }.empty

        String plainXml = workingFile.text
        String encryptedXml = tool.encryptLoginIdentityProviders(plainXml, KEY_HEX)
        logger.info("Encrypted XML: \n${encryptedXml}")

        // Act
        def serializedLines = tool.serializeLoginIdentityProvidersAndPreserveFormat(encryptedXml, workingFile)
        logger.info("Serialized lines: \n${serializedLines.join("\n")}")

        // Assert

        // Some empty lines will be removed
        def trimmedLines = lines.collect { it.trim() }.findAll { it }
        def trimmedSerializedLines = serializedLines.collect { it.trim() }.findAll { it }
        assert trimmedLines.size() == trimmedSerializedLines.size()

        // Ensure the replacement actually occurred
        assert trimmedSerializedLines.findAll { it =~ "encryption=" }.size() == LIP_PASSWORD_LINE_COUNT
    }

    @Test
    void testSerializeLoginIdentityProvidersAndPreserveFormatShouldHandleCommentedFile() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-commented.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // If no sensitive properties are found, the original input text is just returned (comments and formatting in tact)
        def encryptedLines = tool.encryptLoginIdentityProviders(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")
        assert encryptedLines == lines

        // Act
        def serializedLines = ConfigEncryptionTool.serializeLoginIdentityProvidersAndPreserveFormat(encryptedLines.join("\n"), workingFile)
        logger.info("Serialized lines: \n${serializedLines.join("\n")}")

        // Assert
        assert serializedLines == encryptedLines
    }

    @Test
    void testSerializeLoginIdentityProvidersAndPreserveFormatShouldHandleManyProviders() {
        // Arrange
        String loginIdentityProvidersPath = "src/test/resources/login-identity-providers-populated-with-many-providers.xml"
        File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(loginIdentityProvidersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        String plainXml = workingFile.text
        String encryptedXml = tool.encryptLoginIdentityProviders(plainXml, KEY_HEX)
        logger.info("Encrypted XML: \n${encryptedXml}")

        // Act
        def serializedLines = tool.serializeLoginIdentityProvidersAndPreserveFormat(encryptedXml, workingFile)
        logger.info("Serialized lines: \n${serializedLines.join("\n")}")

        // Assert

        // Some empty lines will be removed
        def trimmedLines = lines.collect { it.trim() }.findAll { it }
        def trimmedSerializedLines = serializedLines.collect { it.trim() }.findAll { it }
        assert trimmedLines.size() == trimmedSerializedLines.size()

        // Ensure the replacement actually occurred
        assert trimmedSerializedLines.findAll { it =~ "encryption=" }.size() == LIP_PASSWORD_LINE_COUNT
    }

    @Test
    void testSerializeLoginIdentityProvidersAndPreserveFormatShouldHandleEmptyFile() {
        // Arrange
        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        workingFile.createNewFile()
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // If no sensitive properties are found, the original input text is just returned (comments and formatting in tact)
        def encryptedLines = lines
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Act
        def serializedLines = ConfigEncryptionTool.serializeLoginIdentityProvidersAndPreserveFormat(encryptedLines.join("\n"), workingFile)
        logger.info("Serialized lines: \n${serializedLines.join("\n")}")

        // Assert
        assert serializedLines.findAll { it }.isEmpty()
    }

    @Test
    void testSerializeLoginIdentityProvidersAndPreserveFormatShouldHandleComplexProperty() {
        // Arrange
        String providersPath = "src/test/resources/login-identity-providers-populated-complex-filter.xml"
        File providersFile = new File(providersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-providers.xml")
        workingFile.delete()
        Files.copy(providersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def serializedLines = ConfigEncryptionTool.serializeLoginIdentityProvidersAndPreserveFormat(lines.join("\n"), workingFile)
        logger.info("Serialized lines: \n${serializedLines.join("\n")}")

        // Assert
        assert compareXMLFragments(lines.join("\n"), serializedLines.join("\n"))
    }

    @Test
    void testWriteLoginIdentityProvidersShouldHandleUnreadableFile() {
        // Arrange
        String providersPath = "src/test/resources/login-identity-providers-populated.xml"
        File providersFile = new File(providersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-login-identity-providers.xml")
        workingFile.delete()
        Files.copy(providersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        tool.loginIdentityProvidersPath = workingFile.path
        String writtenPath = "target/tmp/tmp-login-identity-providers-written.xml"
        tool.outputLoginIdentityProvidersPath = writtenPath

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        String plainXml = workingFile.text
        String encryptedXml = tool.encryptLoginIdentityProviders(plainXml, KEY_HEX)
        logger.info("Encrypted XML: \n${encryptedXml}")

        tool.loginIdentityProviders = encryptedXml

        // Remove the working file (simulating an external process)
        workingFile.delete()

        // Act
        tool.writeLoginIdentityProviders()

        // Assert
        File writtenFile = new File(writtenPath)
        List<String> writtenLines = writtenFile.readLines()
        logger.info("Written lines: \n${writtenLines.join("\n")}")

        // The output should contain only what was explicitly serialized in this operation (no pre-existing content)
        assert writtenLines.join("\n") == encryptedXml.trim()

        // Ensure the replacement actually occurred
        assert writtenLines.findAll { it =~ "encryption=" }.size() == LIP_PASSWORD_LINE_COUNT
    }

    @Test
    void testShouldPerformFullOperationForLoginIdentityProviders() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = setupTmpDir()

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_root_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        File inputLIPFile = new File("src/test/resources/login-identity-providers-populated.xml")
        File outputLIPFile = new File("target/tmp/tmp-lip.xml")
        outputLIPFile.delete()

        String originalXmlContent = inputLIPFile.text
        logger.info("Original XML content: ${originalXmlContent}")

        String[] args = ["-l", inputLIPFile.path, "-b", bootstrapFile.path, "-i", outputLIPFile.path, "-k", KEY_HEX, "-v"]

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(ConfigEncryptionTool.DEFAULT_PROTECTION_SCHEME)

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final String updatedXmlContent = outputLIPFile.text
                logger.info("Updated XML content: ${updatedXmlContent}")

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                def originalParsedXml = new XmlSlurper().parseText(originalXmlContent)
                def updatedParsedXml = new XmlSlurper().parseText(updatedXmlContent)
                assert originalParsedXml != updatedParsedXml
                assert originalParsedXml.'**'.findAll { it.@encryption } != updatedParsedXml.'**'.findAll {
                    it.@encryption
                }

                def encryptedValues = updatedParsedXml.provider.find {
                    it.identifier == 'ldap-provider'
                }.property.findAll {
                    it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
                }

                encryptedValues.each {
                    assert spp.unprotect((String) it.text(), (ProtectedPropertyContext) ldapPropertyContext((String) it.@name)) == PASSWORD
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputLIPFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        // Act
        ConfigEncryptionTool.main(args)
        logger.info("Invoked #main with ${args.join(" ")}")

        // Assert

        // Assertions defined above
    }

    @Test
    void testShouldPerformFullOperationMigratingLoginIdentityProviders() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = setupTmpDir()

        // Start with 128-bit encryption and go to whatever is supported on this system
        File emptyKeyFile = new File("src/test/resources/bootstrap_with_root_key_128.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX_128

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + PASSWORD_KEY_HEX

        File inputLIPFile = new File("src/test/resources/login-identity-providers-populated-encrypted.xml")
        File outputLIPFile = new File("target/tmp/tmp-lip.xml")
        outputLIPFile.delete()

        String originalXmlContent = inputLIPFile.text
        logger.info("Original XML content: ${originalXmlContent}")

        // Migrate from KEY_HEX_128 to PASSWORD_KEY_HEX
        String[] args = ["-l", inputLIPFile.path, "-b", bootstrapFile.path, "-i", outputLIPFile.path, "-m", "-e", KEY_HEX_128, "-k", PASSWORD_KEY_HEX, "-v"]

        final SensitivePropertyProvider spp = StandardSensitivePropertyProviderFactory.withKey(PASSWORD_KEY_HEX)
                .getProvider(ConfigEncryptionTool.DEFAULT_PROTECTION_SCHEME)

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final String updatedXmlContent = outputLIPFile.text
                logger.info("Updated XML content: ${updatedXmlContent}")

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                def originalParsedXml = new XmlSlurper().parseText(originalXmlContent)
                def updatedParsedXml = new XmlSlurper().parseText(updatedXmlContent)
                assert originalParsedXml != updatedParsedXml

                def encryptedValues = updatedParsedXml.provider.find {
                    it.identifier == 'ldap-provider'
                }.property.findAll {
                    it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
                }

                encryptedValues.each {
                    assert spp.unprotect((String) it.text(), (ProtectedPropertyContext) ldapPropertyContext((String) it.@name)) == PASSWORD
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputLIPFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        ConfigEncryptionTool.main(args)
    }

    @Test
    void testShouldDecryptAuthorizers() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-encrypted.xml"
        String propertyName = "Manager Password"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Sanity check for decryption
        String cipherText = "q4r7WIgN0MaxdAKM||SGgdCTPGSFEcuH4RraMYEdeyVbOx93abdWTVSWvh1w+klA"
        String EXPECTED_PASSWORD = "thisIsABadPassword"
        final SensitivePropertyProvider spp = StandardSensitivePropertyProviderFactory.withKey(KEY_HEX_128).getProvider(tool.protectionScheme)
        assert spp.unprotect(cipherText, ldapPropertyContext(propertyName)) == EXPECTED_PASSWORD

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def decryptedLines = tool.decryptAuthorizers(lines.join("\n")).split("\n")
        logger.info("Decrypted lines: \n${decryptedLines.join("\n")}")

        // Assert
        def passwordLines = decryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
        assert passwordLines.every { it =~ ">thisIsABadPassword<" }
        // Some lines were not encrypted originally so the encryption attribute would not have been updated
        assert passwordLines.any { it =~ "encryption=\"none\"" }
    }

    @Test
    void testShouldDecryptAzureAuthorizers() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-encrypted-azure.xml"
        String propertyName = "Manager Password"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Sanity check for decryption
        String cipherText = "zOfN7Fy4XnHmd/x0||rTLB/FXB4CLFsCunBwtNMR2hYtj7CdvgF7iFgNNr49g1pw=="
        String EXPECTED_PASSWORD = "thisIsABadPassword"
        final SensitivePropertyProvider spp = StandardSensitivePropertyProviderFactory.withKey(KEY_HEX_128).getProvider(tool.protectionScheme)

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()

        // Act
        def decryptedLines = tool.decryptAuthorizers(lines.join("\n")).split("\n")

        // Assert
        def passwordLines = decryptedLines.findAll { it =~ PASSWORD_PROP_REGEX || it =~ SECRET_PROP_REGEX }
        assert passwordLines.size() == AUTHORIZERS_PASSWORD_LINE_COUNT + AUTHORIZERS_SECRET_LINE_COUNT
        assert passwordLines.every { it =~ EXPECTED_PASSWORD }
        // Some lines were not encrypted originally so the encryption attribute would not have been updated
        assert passwordLines.any { it =~ "encryption=\"none\"" }
    }

    @Test
    void testShouldDecryptAuthorizersWithMultilineElements() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-encrypted-multiline.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def decryptedLines = tool.decryptAuthorizers(lines.join("\n")).split("\n")
        logger.info("Decrypted lines: \n${decryptedLines.join("\n")}")

        // Assert
        def passwordLines = decryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
        assert passwordLines.every { it =~ ">thisIsABadPassword<" }
        // Some lines were not encrypted originally so the encryption attribute would not have been updated
        assert passwordLines.any { it =~ "encryption=\"none\"" }
    }

    @Test
    void testShouldDecryptAuthorizersWithMultipleElementsPerLine() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-encrypted-multiple-per-line.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def decryptedLines = tool.decryptAuthorizers(lines.join("\n")).split("\n")
        logger.info("Decrypted lines: \n${decryptedLines.join("\n")}")

        // Assert
        def passwordLines = decryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
        assert passwordLines.every { it =~ ">thisIsABadPassword<" }
        // Some lines were not encrypted originally so the encryption attribute would not have been updated
        assert passwordLines.any { it =~ "encryption=\"none\"" }
    }


    @Test
    void testDecryptAuthorizersShouldHandleCommentedElements() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-commented.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def decryptedLines = tool.decryptAuthorizers(lines.join("\n")).split("\n")
        logger.info("Decrypted lines: \n${decryptedLines.join("\n")}")

        // Assert

        // If no encrypted properties are found, the original input text is just returned (comments and formatting in tact)
        assert decryptedLines == lines
    }

    @Test
    void testShouldEncryptAuthorizers() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        String encryptionScheme = "encryption=\"aes/gcm/${getKeyLength(KEY_HEX)}\""

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)

        // Act
        def encryptedLines = tool.encryptAuthorizers(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert
        def passwordLines = encryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
        assert passwordLines.every { !it.contains(">thisIsABadPassword<") }
        assert passwordLines.every { it.contains(encryptionScheme) }
        passwordLines.each {
            String ct = (it =~ ">(.*)</property>")[0][1]
            String propertyName = (it =~ 'name="(.*)"')[0][1]
            logger.info("Cipher text: ${ct}")
            assert spp.unprotect(ct, ldapPropertyContext(propertyName)) == PASSWORD
        }
    }

    @Test
    void testShouldEncryptAzureAuthorizer() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-azure.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        String encryptionScheme = "encryption=\"aes/gcm/${getKeyLength(KEY_HEX)}\""

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)

        // Act
        def encryptedLines = tool.encryptAuthorizers(lines.join("\n")).split("\n")

        // Assert
        def passwordLines = encryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        def secretLines = encryptedLines.findAll { it =~ SECRET_PROP_REGEX }
        assert passwordLines.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
        assert secretLines.size() == AUTHORIZERS_SECRET_LINE_COUNT
        def combinedLines = passwordLines + secretLines
        assert combinedLines.every { !it.contains(">thisIsABadPassword<") }
        assert combinedLines.every { it.contains(encryptionScheme) }
        combinedLines.each {
            String ct = (it =~ ">(.*)</property>")[0][1]
            String propertyName = (it =~ 'name="(.*)"')[0][1]
            logger.info("Cipher text: ${ct}")
            assert spp.unprotect(ct, ldapPropertyContext(propertyName)) == PASSWORD
        }
    }

    @Test
    void testShouldEncryptAuthorizersWithEmptySensitiveElements() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-empty.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        String encryptionScheme = "encryption=\"aes/gcm/${getKeyLength(KEY_HEX)}\""

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)

        // Act
        def encryptedLines = tool.encryptAuthorizers(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert
        def passwordLines = encryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
        def populatedPasswordLines = passwordLines.findAll { it =~ />.+</ }
        assert populatedPasswordLines.every { !it.contains(">thisIsABadPassword<") }
        assert populatedPasswordLines.every { it.contains(encryptionScheme) }
        populatedPasswordLines.each {
            String ct = (it =~ ">(.*)</property>")[0][1]
            String propertyName = (it =~ 'name="(.*)"')[0][1]
            logger.info("Cipher text: ${ct}")
            assert spp.unprotect(ct, ldapPropertyContext(propertyName)) == PASSWORD
        }
    }

    @Test
    void testShouldEncryptAuthorizersWithMultilineElements() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-multiline.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        String encryptionScheme = "encryption=\"aes/gcm/${getKeyLength(KEY_HEX)}\""

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)

        // Act
        def encryptedLines = tool.encryptAuthorizers(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert
        def passwordLines = encryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
        assert passwordLines.every { !it.contains(">thisIsABadPassword<") }
        assert passwordLines.every { it.contains(encryptionScheme) }
        passwordLines.each {
            String ct = (it =~ ">(.*)</property>")[0][1]
            String propertyName = (it =~ 'name="(.*)"')[0][1]
            logger.info("Cipher text: ${ct}")
            assert spp.unprotect(ct, ldapPropertyContext(propertyName)) == PASSWORD
        }
    }

    @Test
    void testShouldEncryptAuthorizersWithMultipleElementsPerLine() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-multiple-per-line.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        String encryptionScheme = "encryption=\"aes/gcm/${getKeyLength(KEY_HEX)}\""

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)

        // Act
        def encryptedLines = tool.encryptAuthorizers(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert
        def passwordLines = encryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
        assert passwordLines.every { !it.contains(">thisIsABadPassword<") }
        assert passwordLines.every { it.contains(encryptionScheme) }
        passwordLines.each {
            String ct = (it =~ ">(.*)</property>")[0][1]
            String propertyName = (it =~ 'name="(.*)"')[0][1]
            logger.info("Cipher text: ${ct}")
            assert spp.unprotect(ct, ldapPropertyContext(propertyName)) == PASSWORD
        }
    }

    @Test
    void testShouldEncryptAuthorizersWithRenamedProvider() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-renamed.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        String encryptionScheme = "encryption=\"aes/gcm/${getKeyLength(KEY_HEX)}\""

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")
        assert lines.findAll { it =~ "ldap-user-group-provider" }.empty

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(tool.protectionScheme)

        // Act
        def encryptedLines = tool.encryptAuthorizers(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert
        def passwordLines = encryptedLines.findAll { it =~ PASSWORD_PROP_REGEX }
        assert passwordLines.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
        def populatedPasswordLines = passwordLines.findAll { it =~ />.+</ }
        assert populatedPasswordLines.every { !it.contains(">thisIsABadPassword<") }
        assert populatedPasswordLines.every { it.contains(encryptionScheme) }
        populatedPasswordLines.each {
            String ct = (it =~ ">(.*)</property>")[0][1]
            String propertyName = (it =~ 'name="(.*)"')[0][1]
            logger.info("Cipher text: ${ct}")
            assert spp.unprotect(ct, ldapPropertyContext(propertyName)) == PASSWORD
        }
    }

    @Test
    void testEncryptAuthorizersShouldHandleCommentedElements() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-commented.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX_128

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def encryptedLines = tool.encryptAuthorizers(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Assert

        // If no sensitive properties are found, the original input text is just returned (comments and formatting in tact)
        assert encryptedLines == lines
    }

    @Test
    void testSerializeAuthorizersAndPreserveFormatShouldRespectComments() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        String plainXml = workingFile.text
        String encryptedXml = tool.encryptAuthorizers(plainXml, KEY_HEX)
        logger.info("Encrypted XML: \n${encryptedXml}")

        // Act
        def serializedLines = tool.serializeAuthorizersAndPreserveFormat(encryptedXml, workingFile)
        logger.info("Serialized lines: \n${serializedLines.join("\n")}")

        // Assert

        // Some empty lines will be removed
        def trimmedLines = lines.collect { it.trim() }.findAll { it }
        def trimmedSerializedLines = serializedLines.collect { it.trim() }.findAll { it }
        assert trimmedLines.size() == trimmedSerializedLines.size()

        // Ensure the replacement actually occurred
        assert trimmedSerializedLines.findAll { it =~ "encryption=" }.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
    }

    @Test
    void testSerializeAuthorizersShouldOutputAzureProvider() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-azure.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        def lines = workingFile.readLines()

        String plainXml = workingFile.text
        String encryptedXml = tool.encryptAuthorizers(plainXml, KEY_HEX)

        // Act
        def serializedLines = tool.serializeAuthorizersAndPreserveFormat(encryptedXml, workingFile)

        // Assert

        // Some empty lines will be removed
        def trimmedLines = lines.collect { it.trim() }.findAll { it }
        def trimmedSerializedLines = serializedLines.collect { it.trim() }.findAll { it }
        assert trimmedLines.size() == trimmedSerializedLines.size()

        // Ensure the replacement actually occurred
        assert trimmedSerializedLines.findAll { it =~ "encryption=" }.size() == AUTHORIZERS_PASSWORD_LINE_COUNT + AUTHORIZERS_SECRET_LINE_COUNT
        assert trimmedSerializedLines.find {it =~ ConfigEncryptionTool.LDAP_USER_GROUP_PROVIDER_CLASS }
        assert trimmedSerializedLines.find {it =~ ConfigEncryptionTool.AZURE_USER_GROUP_PROVIDER_CLASS }
    }

    @Test
    void testSerializeAuthorizersAndPreserveFormatShouldHandleRenamedProvider() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-renamed.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")
        assert lines.findAll { it =~ "ldap-user-group-provider" }.empty

        String plainXml = workingFile.text
        String encryptedXml = tool.encryptAuthorizers(plainXml, KEY_HEX)
        logger.info("Encrypted XML: \n${encryptedXml}")

        // Act
        def serializedLines = tool.serializeAuthorizersAndPreserveFormat(encryptedXml, workingFile)
        logger.info("Serialized lines: \n${serializedLines.join("\n")}")

        // Assert

        // Some empty lines will be removed
        def trimmedLines = lines.collect { it.trim() }.findAll { it }
        def trimmedSerializedLines = serializedLines.collect { it.trim() }.findAll { it }
        assert trimmedLines.size() == trimmedSerializedLines.size()

        // Ensure the replacement actually occurred
        assert trimmedSerializedLines.findAll { it =~ "encryption=" }.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
    }

    @Test
    void testSerializeAuthorizersAndPreserveFormatShouldHandleCommentedFile() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-commented.xml"
        File authorizersFile = new File(authorizersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // If no sensitive properties are found, the original input text is just returned (comments and formatting in tact)
        def encryptedLines = tool.encryptAuthorizers(lines.join("\n")).split("\n")
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")
        assert encryptedLines == lines

        // Act
        def serializedLines = ConfigEncryptionTool.serializeAuthorizersAndPreserveFormat(encryptedLines.join("\n"), workingFile)
        logger.info("Serialized lines: \n${serializedLines.join("\n")}")

        // Assert
        assert serializedLines == encryptedLines
    }

    @Test
    void testSerializeAuthorizersAndPreserveFormatShouldHandleEmptyFile() {
        // Arrange
        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        workingFile.createNewFile()
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // If no sensitive properties are found, the original input text is just returned (comments and formatting in tact)
        def encryptedLines = lines
        logger.info("Encrypted lines: \n${encryptedLines.join("\n")}")

        // Act
        def serializedLines = ConfigEncryptionTool.serializeAuthorizersAndPreserveFormat(encryptedLines.join("\n"), workingFile)
        logger.info("Serialized lines: \n${serializedLines.join("\n")}")

        // Assert
        assert serializedLines.findAll { it }.isEmpty()
    }

    @Test
    void testWriteAuthorizersShouldHandleUnreadableFile() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated.xml"
        File authorizersFile = new File(authorizersPath)

        setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX
        tool.authorizersPath = workingFile.path
        String writtenPath = "target/tmp/tmp-authorizers-written.xml"
        tool.outputAuthorizersPath = writtenPath

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        String plainXml = workingFile.text
        String encryptedXml = tool.encryptAuthorizers(plainXml, KEY_HEX)
        logger.info("Encrypted XML: \n${encryptedXml}")

        tool.authorizers = encryptedXml

        // Remove the working file (simulating an external process)
        workingFile.delete()

        // Act
        tool.writeAuthorizers()

        // Assert
        File writtenFile = new File(writtenPath)
        List<String> writtenLines = writtenFile.readLines()
        logger.info("Written lines: \n${writtenLines.join("\n")}")

        // The output should contain only what was explicitly serialized in this operation (no pre-existing content)
        assert writtenLines.join("\n") == encryptedXml.trim()

        // Ensure the replacement actually occurred
        assert writtenLines.findAll { it =~ "encryption=" }.size() == AUTHORIZERS_PASSWORD_LINE_COUNT
    }

    @Test
    void testSerializeAuthorizersAndPreserveFormatShouldHandleComplexProperty() {
        // Arrange
        String authorizersPath = "src/test/resources/authorizers-populated-complex-filter.xml"
        File authorizersFile = new File(authorizersPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-authorizers.xml")
        workingFile.delete()
        Files.copy(authorizersFile.toPath(), workingFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        tool.keyHex = KEY_HEX

        def lines = workingFile.readLines()
        logger.info("Read lines: \n${lines.join("\n")}")

        // Act
        def serializedLines = ConfigEncryptionTool.serializeAuthorizersAndPreserveFormat(lines.join("\n"), workingFile)
        logger.info("Serialized lines: \n${serializedLines.join("\n")}")

        // Assert
        assert compareXMLFragments(lines.join("\n"), serializedLines.join("\n"))
    }

    @Test
    void testShouldPerformFullOperationForAuthorizers() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = setupTmpDir()

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_root_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        File inputAuthorizersFile = new File("src/test/resources/authorizers-populated.xml")
        File outputAuthorizersFile = new File("target/tmp/tmp-authorizers.xml")
        outputAuthorizersFile.delete()

        String originalXmlContent = inputAuthorizersFile.text
        logger.info("Original XML content: ${originalXmlContent}")

        String[] args = ["-a", inputAuthorizersFile.path, "-b", bootstrapFile.path, "-u", outputAuthorizersFile.path, "-k", KEY_HEX, "-v"]

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(ConfigEncryptionTool.DEFAULT_PROTECTION_SCHEME)

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final String updatedXmlContent = outputAuthorizersFile.text
                logger.info("Updated XML content: ${updatedXmlContent}")

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                def originalParsedXml = new XmlSlurper().parseText(originalXmlContent)
                def updatedParsedXml = new XmlSlurper().parseText(updatedXmlContent)
                assert originalParsedXml != updatedParsedXml
                assert originalParsedXml.'**'.findAll { it.@encryption } != updatedParsedXml.'**'.findAll {
                    it.@encryption
                }

                def encryptedValues = updatedParsedXml.userGroupProvider.find {
                    it.identifier == 'ldap-user-group-provider'
                }.property.findAll {
                    it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
                }

                encryptedValues.each {
                    assert spp.unprotect((String) it.text(), (ProtectedPropertyContext) ldapPropertyContext((String) it.@name)) == PASSWORD
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputAuthorizersFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        // Act
        ConfigEncryptionTool.main(args)
        logger.info("Invoked #main with ${args.join(" ")}")

        // Assert

        // Assertions defined above
    }

    @Test
    void testShouldPerformFullOperationMigratingAuthorizers() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = setupTmpDir()

        // Start with 128-bit encryption and go to whatever is supported on this system
        File emptyKeyFile = new File("src/test/resources/bootstrap_with_root_key_128.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX_128

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + PASSWORD_KEY_HEX

        File inputAuthorizersFile = new File("src/test/resources/authorizers-populated-encrypted.xml")
        File outputAuthorizersFile = new File("target/tmp/tmp-authorizers.xml")
        outputAuthorizersFile.delete()

        String originalXmlContent = inputAuthorizersFile.text
        logger.info("Original XML content: ${originalXmlContent}")

        // Migrate from KEY_HEX_128 to PASSWORD_KEY_HEX
        String[] args = ["-a", inputAuthorizersFile.path, "-b", bootstrapFile.path, "-u", outputAuthorizersFile.path, "-m", "-e", KEY_HEX_128, "-k", PASSWORD_KEY_HEX, "-v"]

        final SensitivePropertyProvider spp = StandardSensitivePropertyProviderFactory.withKey(PASSWORD_KEY_HEX)
                .getProvider(ConfigEncryptionTool.DEFAULT_PROTECTION_SCHEME)

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final String updatedXmlContent = outputAuthorizersFile.text
                logger.info("Updated XML content: ${updatedXmlContent}")

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                def originalParsedXml = new XmlSlurper().parseText(originalXmlContent)
                def updatedParsedXml = new XmlSlurper().parseText(updatedXmlContent)
                assert originalParsedXml != updatedParsedXml

                def encryptedValues = updatedParsedXml.userGroupProvider.find {
                    it.identifier == 'ldap-user-group-provider'
                }.property.findAll {
                    it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
                }

                encryptedValues.each {
                    assert spp.unprotect((String) it.text(), (ProtectedPropertyContext) ldapPropertyContext((String) it.@name)) == PASSWORD
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputAuthorizersFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        // Act
        ConfigEncryptionTool.main(args)
        logger.info("Invoked #main with ${args.join(" ")}")

        // Assert

        // Assertions defined above
    }

    @Test
    void testShouldPerformFullOperationForAuthorizersWithComplexUserSearchFilter() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = setupTmpDir()

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_root_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        File inputAuthorizersFile = new File("src/test/resources/authorizers-populated-complex-filter.xml")
        File outputAuthorizersFile = new File("target/tmp/tmp-authorizers.xml")
        outputAuthorizersFile.delete()

        String originalXmlContent = inputAuthorizersFile.text
        logger.info("Original XML content: ${originalXmlContent}")

        String[] args = ["-a", inputAuthorizersFile.path, "-b", bootstrapFile.path, "-u", outputAuthorizersFile.path, "-k", KEY_HEX, "-v"]

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(ConfigEncryptionTool.DEFAULT_PROTECTION_SCHEME)

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final String updatedXmlContent = outputAuthorizersFile.text
                logger.info("Updated XML content: ${updatedXmlContent}")

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                def originalParsedXml = new XmlSlurper().parseText(originalXmlContent)
                def updatedParsedXml = new XmlSlurper().parseText(updatedXmlContent)
                assert originalParsedXml != updatedParsedXml
                assert originalParsedXml.'**'.findAll { it.@encryption } != updatedParsedXml.'**'.findAll {
                    it.@encryption
                }

                def encryptedValues = updatedParsedXml.userGroupProvider.find {
                    it.identifier == 'ldap-user-group-provider'
                }.property.findAll {
                    it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
                }

                encryptedValues.each {
                    assert spp.unprotect((String) it.text(), (ProtectedPropertyContext) ldapPropertyContext((String) it.@name)) == PASSWORD
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputAuthorizersFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        // Act
        ConfigEncryptionTool.main(args)
        logger.info("Invoked #main with ${args.join(" ")}")

        // Assert

        // Assertions defined above
    }

    @Test
    void testShouldPerformFullOperationForNiFiPropertiesAndLoginIdentityProvidersAndAuthorizers() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = setupTmpDir()

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_root_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        // Set up the NFP file
        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File outputPropertiesFile = new File("target/tmp/tmp_nifi.properties")
        outputPropertiesFile.delete()

        NiFiProperties inputProperties = new NiFiPropertiesLoader().load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        // Set up the LIP file
        File inputLIPFile = new File("src/test/resources/login-identity-providers-populated.xml")
        File outputLIPFile = new File("target/tmp/tmp-lip.xml")
        outputLIPFile.delete()

        String originalLipXmlContent = inputLIPFile.text
        logger.info("Original LIP XML content: ${originalLipXmlContent}")

        // Set up the Authorizers file
        File inputAuthorizersFile = new File("src/test/resources/authorizers-populated.xml")
        File outputAuthorizersFile = new File("target/tmp/tmp-authorizers.xml")
        outputAuthorizersFile.delete()

        String originalAuthorizersXmlContent = inputAuthorizersFile.text
        logger.info("Original Authorizers XML content: ${originalAuthorizersXmlContent}")

        String[] args = [
                "-n", inputPropertiesFile.path,
                "-l", inputLIPFile.path,
                "-a", inputAuthorizersFile.path,
                "-b", bootstrapFile.path,
                "-o", outputPropertiesFile.path,
                "-i", outputLIPFile.path,
                "-u", outputAuthorizersFile.path,
                "-k", KEY_HEX,
                "-v"]

        final SensitivePropertyProvider spp = DEFAULT_PROVIDER_FACTORY.getProvider(ConfigEncryptionTool.DEFAULT_PROTECTION_SCHEME)

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {

                /*** NiFi Properties Assertions ***/

                final List<String> updatedPropertiesLines = outputPropertiesFile.readLines()
                logger.info("Updated nifi.properties:")
                logger.info("\n" * 2 + updatedPropertiesLines.join("\n"))

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                NiFiProperties updatedProperties = new NiFiPropertiesLoader().loadProtectedProperties(outputPropertiesFile)
                assert updatedProperties.size() >= inputProperties.size()
                originalSensitiveValues.every { String key, String originalValue ->
                    assert updatedProperties.getProperty(key) != originalValue
                }

                // Check that the new NiFiProperties instance matches the output file (values still encrypted)
                updatedProperties.getPropertyKeys().every { String key ->
                    assert updatedPropertiesLines.contains("${key}=${updatedProperties.getProperty(key)}".toString())
                }

                /*** Login Identity Providers Assertions ***/

                final String updatedLipXmlContent = outputLIPFile.text
                logger.info("Updated LIP XML content: ${updatedLipXmlContent}")
                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                def originalLipParsedXml = new XmlSlurper().parseText(originalLipXmlContent)
                def updatedLipParsedXml = new XmlSlurper().parseText(updatedLipXmlContent)
                assert originalLipParsedXml != updatedLipParsedXml
                assert originalLipParsedXml.'**'.findAll { it.@encryption } != updatedLipParsedXml.'**'.findAll {
                    it.@encryption
                }
                def lipEncryptedValues = updatedLipParsedXml.provider.find {
                    it.identifier == 'ldap-provider'
                }.property.findAll {
                    it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
                }
                lipEncryptedValues.each {
                    assert spp.unprotect((String) it.text(), (ProtectedPropertyContext) ldapPropertyContext((String) it.@name)) == PASSWORD
                }
                // Check that the comments are still there
                def lipTrimmedLines = inputLIPFile.readLines().collect { it.trim() }.findAll { it }
                def lipTrimmedSerializedLines = updatedLipXmlContent.split("\n").collect { it.trim() }.findAll { it }
                assert lipTrimmedLines.size() == lipTrimmedSerializedLines.size()

                /*** Authorizers Assertions ***/

                final String updatedAuthorizersXmlContent = outputAuthorizersFile.text
                logger.info("Updated Authorizers XML content: ${updatedAuthorizersXmlContent}")
                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                def originalAuthorizersParsedXml = new XmlSlurper().parseText(originalAuthorizersXmlContent)
                def updatedAuthorizersParsedXml = new XmlSlurper().parseText(updatedAuthorizersXmlContent)
                assert originalAuthorizersParsedXml != updatedAuthorizersParsedXml
                assert originalAuthorizersParsedXml.'**'.findAll {
                    it.@encryption
                } != updatedAuthorizersParsedXml.'**'.findAll {
                    it.@encryption
                }
                def authorizersEncryptedValues = updatedAuthorizersParsedXml.userGroupProvider.find {
                    it.identifier == 'ldap-user-group-provider'
                }.property.findAll {
                    it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
                }
                authorizersEncryptedValues.each {
                    assert spp.unprotect((String) it.text(), (ProtectedPropertyContext) ldapPropertyContext((String) it.@name)) == PASSWORD
                }
                // Check that the comments are still there
                def authorizersTrimmedLines = inputAuthorizersFile.readLines().collect { it.trim() }.findAll { it }
                def authorizersTrimmedSerializedLines = updatedAuthorizersXmlContent.split("\n").collect {
                    it.trim()
                }.findAll { it }
                assert authorizersTrimmedLines.size() == authorizersTrimmedSerializedLines.size()

                /*** Bootstrap assertions ***/

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputPropertiesFile.deleteOnExit()
                outputLIPFile.deleteOnExit()
                outputAuthorizersFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        // Act
        ConfigEncryptionTool.main(args)
        logger.info("Invoked #main with ${args.join(" ")}")

        // Assert

        // Assertions defined above
    }

    @Test
    void testParseShouldIgnoreFilesIfOverrideFlagPresent() {
        // Arrange
        String niFiPropertiesPath = "conf/nifi.properties"
        String flowXmlPath = "conf/flow.xml.gz"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse("-n ${niFiPropertiesPath} -f ${flowXmlPath} -x".split(" ") as String[])

        // Assert
        assert !tool.handlingNiFiProperties
        assert !tool.handlingLoginIdentityProviders
        assert tool.handlingFlowXml
    }

    @Test
    void testParseShouldWarnIfFlowXmlWillBeOverwritten() {
        // Arrange
        String niFiPropertiesPath = "conf/nifi.properties"
        String flowXmlPath = "conf/flow.xml.gz"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse("-n ${niFiPropertiesPath} -f ${flowXmlPath}".split(" ") as String[])
    }

    @Test
    void testParseShouldFailOnFlowWithoutNiFiProperties() {
        // Arrange
        String flowXmlPath = "conf/flow.xml.gz"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        Assertions.assertThrows(CommandLineParseException.class, () -> tool.parse("-f ${flowXmlPath} -x".split(" ") as String[]))
    }

    @Test
    void testShouldLoadFlowXmlContent() {
        // Arrange
        String flowXmlPath = "src/test/resources/flow.xml"
        File flowXmlFile = new File(flowXmlPath)

        String flowXmlGzPath = "src/test/resources/flow.xml.gz"
        File flowXmlGzFile = new File(flowXmlGzPath)

        File tmpDir = setupTmpDir()

        File workingFile = new File("target/tmp/tmp-flow.xml")
        workingFile.delete()
        Files.copy(flowXmlFile.toPath(), workingFile.toPath())
        File workingGzFile = new File("target/tmp/tmp-flow.xml.gz")
        workingGzFile.delete()
        Files.copy(flowXmlGzFile.toPath(), workingGzFile.toPath())
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true

        String xmlContent = workingFile.text
        logger.info("Read flow.xml: \n${xmlContent}")

        // Act
        InputStream xmlContentStream = tool.loadFlowXml(workingGzFile.path)
        String readXmlContent = IOUtils.toString(xmlContentStream, StandardCharsets.UTF_8)
        
        logger.info("Loaded flow.xml.gz: \n${readXmlContent}")

        // Assert
        assert readXmlContent.trim() == xmlContent.trim()
    }

    @Test
    void testShouldDetectActionFlags() {
        // Arrange
        final def HELP_AND_VERBOSE_ARGS = [["-h", "--help"], ["-v", "--verbose"]]
        final List<String> IGNORED_ARGS = ["translateCli"]

        // Create a list with combinations of h[elp] and v[erbose], individual flags, and empty flag
        def args = GroovyCollections.combinations(HELP_AND_VERBOSE_ARGS as Iterable) + HELP_AND_VERBOSE_ARGS.flatten().collect {
            [it]
        } + [[""]]
        String acceptableArg = "--translateCli"
        String unacceptableArg = "--migrate"

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.isVerbose = true
        CommandLineParser parser = new DefaultParser()

        // Act
        args.each { List<String> invocationArgs ->
            // Run each scenario with an allowed argument and without
            [IGNORED_ARGS, []].each { List<String> acceptableArgs ->
                // Check ""/-h/-v alone
                logger.info("Checking '${invocationArgs.join(" ")}' with acceptable args: ${acceptableArgs}")
                CommandLine commandLine = parser.parse(ConfigEncryptionTool.getCliOptions(), invocationArgs as String[])
                boolean cleanRun = tool.commandLineHasActionFlags(commandLine, acceptableArgs)
                logger.info("Clean run has action flags: ${cleanRun} | Expected: false")

                // Check with an allowed/ignored arg
                def allowedArgs = invocationArgs + acceptableArg
                logger.info("Checking '${allowedArgs.join(" ")}' with acceptable args: ${acceptableArgs}")
                commandLine = parser.parse(ConfigEncryptionTool.getCliOptions(), allowedArgs as String[])
                boolean allowedRun = tool.commandLineHasActionFlags(commandLine, acceptableArgs)
                logger.info("Allowed run has action flags: ${allowedRun} | Expected: ${acceptableArgs.isEmpty().toString()}")

                // Check with an unallowed arg
                def unallowedArgs = invocationArgs + unacceptableArg
                logger.info("Checking '${unallowedArgs.join(" ")}' with acceptable args: ${acceptableArgs}")
                commandLine = parser.parse(ConfigEncryptionTool.getCliOptions(), unallowedArgs as String[])
                boolean unallowedRun = tool.commandLineHasActionFlags(commandLine, acceptableArgs)
                logger.info("Unallowed run has action flags: ${unallowedRun} | Expected: true")

                // Assert
                assert !cleanRun
                assert allowedRun == acceptableArgs.isEmpty()
                assert unallowedRun
            }
        }
    }

    @Test
    void testShouldTranslateCliWithPlaintextInput() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        final Map<String, String> EXPECTED_CLI_OUTPUT = [
                "baseUrl"         : "https://nifi.nifi.apache.org:8443",
                "keystore"        : "/path/to/keystore.jks",
                "keystoreType"    : "JKS",
                "keystorePasswd"  : "thisIsABadKeystorePassword",
                "keyPasswd"       : "thisIsABadKeyPassword",
                "truststore"      : "",
                "truststoreType"  : "",
                "truststorePasswd": "",
                "proxiedEntity"   : "",
        ]

        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_root_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX

        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")

        NiFiProperties inputProperties = new NiFiPropertiesLoader().load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        String[] args = ["-n", inputPropertiesFile.path, "-b", bootstrapFile.path, "-c"]

        ConfigEncryptionTool.main(args)
    }

    @Test
    void testShouldTranslateCliWithPlaintextInputWithoutBootstrapConf() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        final Map<String, String> EXPECTED_CLI_OUTPUT = [
                "baseUrl"         : "https://nifi.nifi.apache.org:8443",
                "keystore"        : "/path/to/keystore.jks",
                "keystoreType"    : "JKS",
                "keystorePasswd"  : "thisIsABadKeystorePassword",
                "keyPasswd"       : "thisIsABadKeyPassword",
                "truststore"      : "",
                "truststoreType"  : "",
                "truststorePasswd": "",
                "proxiedEntity"   : "",
        ]

        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])

        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")

        NiFiProperties inputProperties = new NiFiPropertiesLoader().load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        String[] args = ["-n", inputPropertiesFile.path, "-c"]

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final String standardOutput = systemOutRule.getLog()
                List<String> lines = standardOutput.split("\n")

                // The SystemRule log also includes STDERR, so truncate after 9 lines
                def stdoutLines = lines[0..<EXPECTED_CLI_OUTPUT.size()]
                logger.info("STDOUT:\n\t${stdoutLines.join("\n\t")}")

                // Split the output into lines and create a map of the keys and values
                def parsedCli = stdoutLines.collectEntries { String line ->
                    def components = line.split("=", 2)
                    components.size() > 1 ? [(components[0]): components[1]] : [(components[0]): ""]
                }

                assert parsedCli.size() == EXPECTED_CLI_OUTPUT.size()
                assert EXPECTED_CLI_OUTPUT.every { String k, String v -> parsedCli.get(k) == v }

                // Clean up
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        // Act
        ConfigEncryptionTool.main(args)
        logger.info("Invoked #main with ${args.join(" ")}")

        // Assert

        // Assertions defined above
    }

    @Test
    void testShouldTranslateCliWithEncryptedInput() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        final Map<String, String> EXPECTED_CLI_OUTPUT = [
                "baseUrl"         : "https://nifi.nifi.apache.org:8443",
                "keystore"        : "/path/to/keystore.jks",
                "keystoreType"    : "JKS",
                "keystorePasswd"  : "thisIsABadKeystorePassword",
                "keyPasswd"       : "thisIsABadKeyPassword",
                "truststore"      : "",
                "truststoreType"  : "",
                "truststorePasswd": "",
                "proxiedEntity"   : "",
        ]

        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])

        String bootstrapPath = isUnlimitedStrengthCryptoAvailable() ? "src/test/resources/bootstrap_with_root_key.conf" :
                "src/test/resources/bootstrap_with_root_key_128.conf"
        File rootKeyFile = new File(bootstrapPath)
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(rootKeyFile.toPath(), bootstrapFile.toPath())

        String inputPropertiesPath = isUnlimitedStrengthCryptoAvailable() ? "src/test/resources/nifi_with_sensitive_properties_protected_aes.properties" :
                "src/test/resources/nifi_with_sensitive_properties_protected_aes_128.properties"
        File inputPropertiesFile = new File(inputPropertiesPath)

        NiFiProperties inputProperties = NiFiPropertiesLoader.withKey(KEY_HEX).load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        String[] args = ["-n", inputPropertiesFile.path, "-b", bootstrapFile.path, "-c"]

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final String standardOutput = systemOutRule.getLog()
                List<String> lines = standardOutput.split("\n")

                // The SystemRule log also includes STDERR, so truncate after 9 lines
                def stdoutLines = lines[0..<EXPECTED_CLI_OUTPUT.size()]
                logger.info("STDOUT:\n\t${stdoutLines.join("\n\t")}")

                // Split the output into lines and create a map of the keys and values
                def parsedCli = stdoutLines.collectEntries { String line ->
                    def components = line.split("=", 2)
                    components.size() > 1 ? [(components[0]): components[1]] : [(components[0]): ""]
                }

                assert parsedCli.size() == EXPECTED_CLI_OUTPUT.size()
                assert EXPECTED_CLI_OUTPUT.every { String k, String v -> parsedCli.get(k) == v }

                // Clean up
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        // Act
        ConfigEncryptionTool.main(args)
        logger.info("Invoked #main with ${args.join(" ")}")

        // Assert

        // Assertions defined above
    }

    @Test
    void testTranslateCliWithEncryptedInputShouldNotIntersperseVerboseOutput() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        final Map<String, String> EXPECTED_CLI_OUTPUT = [
                "baseUrl"         : "https://nifi.nifi.apache.org:8443",
                "keystore"        : "/path/to/keystore.jks",
                "keystoreType"    : "JKS",
                "keystorePasswd"  : "thisIsABadKeystorePassword",
                "keyPasswd"       : "thisIsABadKeyPassword",
                "truststore"      : "",
                "truststoreType"  : "",
                "truststorePasswd": "",
                "proxiedEntity"   : "",
        ]

        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])

        String bootstrapPath = isUnlimitedStrengthCryptoAvailable() ? "src/test/resources/bootstrap_with_root_key.conf" :
                "src/test/resources/bootstrap_with_root_key_128.conf"
        File rootKeyFile = new File(bootstrapPath)
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(rootKeyFile.toPath(), bootstrapFile.toPath())

        String inputPropertiesPath = isUnlimitedStrengthCryptoAvailable() ? "src/test/resources/nifi_with_sensitive_properties_protected_aes.properties" :
                "src/test/resources/nifi_with_sensitive_properties_protected_aes_128.properties"
        File inputPropertiesFile = new File(inputPropertiesPath)

        NiFiProperties inputProperties = NiFiPropertiesLoader.withKey(KEY_HEX).load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        String[] args = ["-n", inputPropertiesFile.path, "-b", bootstrapFile.path, "-c", "-v"]

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final String standardOutput = systemOutRule.getLog()
                List<String> lines = standardOutput.split("\n")

                // The SystemRule log also includes STDERR, so truncate after 9 lines
                def stdoutLines = lines[0..<EXPECTED_CLI_OUTPUT.size()]
                logger.info("STDOUT:\n\t${stdoutLines.join("\n\t")}")

                // Split the output into lines and create a map of the keys and values
                def parsedCli = stdoutLines.collectEntries { String line ->
                    def components = line.split("=", 2)
                    components.size() > 1 ? [(components[0]): components[1]] : [(components[0]): ""]
                }

                assert parsedCli.size() == EXPECTED_CLI_OUTPUT.size()
                assert EXPECTED_CLI_OUTPUT.every { String k, String v -> parsedCli.get(k) == v }

                // Clean up
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        // Act
        ConfigEncryptionTool.main(args)
        logger.info("Invoked #main with ${args.join(" ")}")

        // Assert

        // Assertions defined above
    }

    @Test
    void testShouldTranslateCli() {
        // Arrange
        final Map<String, String> EXPECTED_CLI_OUTPUT = [
                "baseUrl"         : "https://nifi.nifi.apache.org:8443",
                "keystore"        : "/path/to/keystore.jks",
                "keystoreType"    : "JKS",
                "keystorePasswd"  : "thisIsABadKeystorePassword",
                "keyPasswd"       : "thisIsABadKeyPassword",
                "truststore"      : "",
                "truststoreType"  : "",
                "truststorePasswd": "",
                "proxiedEntity"   : "",
        ]

        String originalNiFiPropertiesPath = "src/test/resources/nifi_with_sensitive_properties_unprotected.properties"

        NiFiProperties plainProperties = NiFiPropertiesLoader.withKey(KEY_HEX).load(originalNiFiPropertiesPath)
        logger.info("Loaded NiFiProperties from ${originalNiFiPropertiesPath}")

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.translatingCli = true
        tool.niFiProperties = plainProperties

        // Act
        String cliOutput = tool.translateNiFiPropertiesToCLI()
        logger.info("Translated to CLI format: \n${cliOutput}")

        // Assert
        def parsedCli = cliOutput.split("\n").collectEntries { String line ->
            def components = line.split("=", 2)
            [(components[0]): components[1]]
        }

        assert parsedCli.size() == EXPECTED_CLI_OUTPUT.size()
        assert EXPECTED_CLI_OUTPUT.every { String k, String v -> parsedCli.get(k) == v }
    }

    @Test
    void testShouldFailOnCliTranslateIfConflictingFlagsPresent() {
        // Arrange
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        def validOpts = [
                "-n nifi.properties",
                "--niFiProperties nifi.properties",
                "--verbose -n nifi.properties -b bootstrap.conf",
        ]

        // These values won't cause an error in #commandLineHasActionFlags() but will throw an error later in #parse()
        // Don't test with -h/--help because it will cause a System.exit()
        def incompleteOpts = [
                "",
                "-v",
                "--verbose",
//                "-h",
//                "--help",
                "-b bootstrap.conf",
                "--bootstrapConf bootstrap.conf",
        ]

        def invalidOpts = [
                "--migrate",
                "-o output",
                "-x \$s0\$"
        ]

        // Act
        validOpts.each { String valid ->
            tool = new ConfigEncryptionTool()
            def args = (valid + " -c").split(" ")
            logger.info("Testing with ${args}")
            tool.parse(args as String[])
        }

        incompleteOpts.each { String incomplete ->
            tool = new ConfigEncryptionTool()
            def args = (incomplete + " -c").split(" ")
            Assertions.assertThrows(CommandLineParseException.class, () -> tool.parse(args as String[]))
        }

        invalidOpts.each { String invalid ->
            tool = new ConfigEncryptionTool()
            def args = (invalid + " -c").split(" ")
            Assertions.assertThrows(CommandLineParseException.class, () -> tool.parse(args as String[]))
        }
    }

    @Test
    void testTranslateCliShouldFailIfMissingNecessaryFlags() {
        // Arrange
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Bootstrap alone is insufficient; nifi.properties alone is ok if it is in plaintext
        def invalidOpts = [
                "-b bootstrap.conf",
        ]

        // Act
        invalidOpts.each { String invalid ->
            def args = (invalid + " -c").split(" ")
            Assertions.assertThrows(CommandLineParseException.class, () -> tool.parse(args as String[]))
        }
    }

    @Test
    void testFindFieldsInStream() {
        def verifyTool = new ConfigEncryptionTool()
        verifyTool.isVerbose = true
        verifyTool.flowXmlPath = new File("src/test/resources/flow.xml.gz").path
        InputStream updatedFlowXmlContent = verifyTool.loadFlowXml(verifyTool.flowXmlPath)
        Set<String> fieldsFound = findFieldsInStream(updatedFlowXmlContent, WFXCTR)
        assert(fieldsFound.size() > 0)
    }

    static boolean compareXMLFragments(String expectedXML, String actualXML) {
        Diff diffSimilar = DiffBuilder.compare(expectedXML).withTest(actualXML)
                .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byName))
                .ignoreWhitespace().checkForSimilar().build()
        def allDifferences = diffSimilar.getDifferences()
        if (diffSimilar.hasDifferences()) {
            allDifferences.each { diff ->
                logger.info("Difference: ${diff.toString()}")
            }
        }
        !diffSimilar.hasDifferences()
    }

    static Set<String> findFieldsInStream(InputStream fileInputStream, String pattern) {
        Set<String> fieldsFound = new HashSet<String>()
        Reader reader = new BufferedReader(new InputStreamReader(fileInputStream))
        String line
        while((line = reader.readLine()) != null) {
            def matcher = line =~ pattern
            if(matcher.find()) {
                fieldsFound.add(matcher.getAt(0))
            }
        }
        fieldsFound
    }

    private String removeXmlDeclarationAndComments(final String xmlFlow) {
        return xmlFlow.replaceAll("<\\?xml.+\\?>", "").replaceAll("(?s)<!--.*?-->", "")
    }
}
