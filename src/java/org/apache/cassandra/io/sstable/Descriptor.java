/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Objects;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.IMetadataSerializer;
import org.apache.cassandra.io.sstable.metadata.LegacyMetadataSerializer;
import org.apache.cassandra.io.sstable.metadata.MetadataSerializer;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.sstable.Component.separator;

/**
 * A SSTable is described by the keyspace and column family it contains data
 * for, a generation (where higher generations contain more recent data) and
 * an alphabetic version string.
 *
 * A descriptor can be marked as temporary, which influences generated filenames.
 */
public class Descriptor
{
    public static String TMP_EXT = ".tmp";
    private static final Pattern FORWARD_SLASH_PATTERN = Pattern.compile("/");
    private static final Pattern INDEX_DIR_PATTERN = Pattern.compile("\\.");

    /** canonicalized path to the directory where SSTable resides */
    public final File directory;
    /** version has the following format: <code>[a-z]+</code> */
    public final Version version;
    public final String ksname;
    public final String cfname;
    public final int generation;
    public final SSTableFormat.Type formatType;
    /** digest component - might be {@code null} for old, legacy sstables */
    public final Component digestComponent;
    private final int hashCode;

    /**
     * A descriptor that assumes CURRENT_VERSION.
     */
    @VisibleForTesting
    public Descriptor(File directory, String ksname, String cfname, int generation)
    {
        this(SSTableFormat.Type.current().info.getLatestVersion(), directory, ksname, cfname, generation, SSTableFormat.Type.current(), null);
    }

    /**
     * Constructor for sstable writers only.
     */
    public Descriptor(File directory, String ksname, String cfname, int generation, SSTableFormat.Type formatType)
    {
        this(formatType.info.getLatestVersion(), directory, ksname, cfname, generation, formatType, Component.digestFor(formatType.info.getLatestVersion().uncompressedChecksumType()));
    }

    @VisibleForTesting
    public Descriptor(String version, File directory, String ksname, String cfname, int generation, SSTableFormat.Type formatType)
    {
        this(formatType.info.getVersion(version), directory, ksname, cfname, generation, formatType, Component.digestFor(formatType.info.getLatestVersion().uncompressedChecksumType()));
    }

    public Descriptor(Version version, File directory, String ksname, String cfname, int generation, SSTableFormat.Type formatType, Component digestComponent)
    {
        assert version != null && directory != null && ksname != null && cfname != null && formatType.info.getLatestVersion().getClass().equals(version.getClass());
        this.version = version;
        try
        {
            this.directory = directory.getCanonicalFile();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        this.ksname = ksname;
        this.cfname = cfname;
        this.generation = generation;
        this.formatType = formatType;
        this.digestComponent = digestComponent;

        hashCode = Objects.hashCode(version, this.directory, generation, ksname, cfname, formatType);
    }

    public Descriptor withGeneration(int newGeneration)
    {
        return new Descriptor(version, directory, ksname, cfname, newGeneration, formatType, digestComponent);
    }

    public Descriptor withFormatType(SSTableFormat.Type newType)
    {
        return new Descriptor(newType.info.getLatestVersion(), directory, ksname, cfname, generation, newType, digestComponent);
    }

    public Descriptor withDigestComponent(Component newDigestComponent)
    {
        return new Descriptor(version, directory, ksname, cfname, generation, formatType, newDigestComponent);
    }

    public String tmpFilenameFor(Component component)
    {
        return filenameFor(component) + TMP_EXT;
    }

    public String filenameFor(Component component)
    {
        return baseFilename() + separator + component.name();
    }

    public String baseFilename()
    {
        StringBuilder buff = new StringBuilder();
        buff.append(directory).append(File.separatorChar);
        appendFileName(buff);
        return buff.toString();
    }

    private void appendFileName(StringBuilder buff)
    {
        if (!version.hasNewFileName())
        {
            buff.append(ksname).append(separator);
            buff.append(cfname).append(separator);
        }
        buff.append(version).append(separator);
        buff.append(generation);
        if (formatType != SSTableFormat.Type.LEGACY)
            buff.append(separator).append(formatType.name);
    }

    public String relativeFilenameFor(Component component)
    {
        final StringBuilder buff = new StringBuilder();
        if (Directories.isSecondaryIndexFolder(directory))
        {
            buff.append(directory.getName()).append(File.separator);
        }

        appendFileName(buff);
        buff.append(separator).append(component.name());
        return buff.toString();
    }

    public SSTableFormat getFormat()
    {
        return formatType.info;
    }

    /** Return any temporary files found in the directory */
    public List<File> getTemporaryFiles()
    {
        List<File> ret = new ArrayList<>();
        File[] tmpFiles = directory.listFiles((dir, name) ->
                                              name.endsWith(Descriptor.TMP_EXT));

        for (File tmpFile : tmpFiles)
            ret.add(tmpFile);

        return ret;
    }

    /**
     *  Files obsoleted by CASSANDRA-7066 : temporary files and compactions_in_progress. We support
     *  versions 2.1 (ka) and 2.2 (la).
     *  Temporary files have tmp- or tmplink- at the beginning for 2.2 sstables or after ks-cf- for 2.1 sstables
     */

    private final static String LEGACY_COMP_IN_PROG_REGEX_STR = "^compactions_in_progress(\\-[\\d,a-f]{32})?$";
    private final static Pattern LEGACY_COMP_IN_PROG_REGEX = Pattern.compile(LEGACY_COMP_IN_PROG_REGEX_STR);
    private final static String LEGACY_TMP_REGEX_STR = "^((.*)\\-(.*)\\-)?tmp(link)?\\-((?:l|k).)\\-(\\d)*\\-(.*)$";
    private final static Pattern LEGACY_TMP_REGEX = Pattern.compile(LEGACY_TMP_REGEX_STR);

    public static boolean isLegacyFile(File file)
    {
        if (file.isDirectory())
            return file.getParentFile() != null &&
                   file.getParentFile().getName().equalsIgnoreCase("system") &&
                   LEGACY_COMP_IN_PROG_REGEX.matcher(file.getName()).matches();
        else
            return LEGACY_TMP_REGEX.matcher(file.getName()).matches();
    }

    public static boolean isValidFile(String fileName)
    {
        return fileName.endsWith(".db") && !LEGACY_TMP_REGEX.matcher(fileName).matches();
    }

    /**
     * @see #fromFilename(File directory, String name)
     * @param filename The SSTable filename
     * @return Descriptor of the SSTable initialized from filename
     */
    public static Descriptor fromFilename(String filename)
    {
        return fromFilename(filename, false);
    }

    public static Descriptor fromFilename(String filename, SSTableFormat.Type formatType)
    {
        return fromFilename(filename).withFormatType(formatType);
    }

    public static Descriptor fromFilename(String filename, boolean skipComponent)
    {
        File file = new File(filename).getAbsoluteFile();
        return fromFilename(file.getParentFile(), file.getName(), skipComponent).left;
    }

    public static Pair<Descriptor, String> fromFilename(File directory, String name)
    {
        return fromFilename(directory, name, false);
    }

    /**
     * Filename of the form is vary by version:
     *
     * <ul>
     *     <li>&lt;ksname&gt;-&lt;cfname&gt;-(tmp-)?&lt;version&gt;-&lt;gen&gt;-&lt;component&gt; for cassandra 2.0 and before</li>
     *     <li>(&lt;tmp marker&gt;-)?&lt;version&gt;-&lt;gen&gt;-&lt;component&gt; for cassandra 3.0 and later</li>
     * </ul>
     *
     * If this is for SSTable of secondary index, directory should ends with index name for 2.1+.
     *
     * @param directory The directory of the SSTable files
     * @param name The name of the SSTable file
     * @param skipComponent true if the name param should not be parsed for a component tag
     *
     * @return A Descriptor for the SSTable, and the Component remainder.
     */
    public static Pair<Descriptor, String> fromFilename(File directory, String name, boolean skipComponent)
    {
        File parentDirectory = directory != null ? directory : new File(".");

        // tokenize the filename
        StringTokenizer st = new StringTokenizer(name, String.valueOf(separator));
        String nexttok;

        // read tokens backwards to determine version
        Deque<String> tokenStack = new ArrayDeque<>();
        while (st.hasMoreTokens())
        {
            tokenStack.push(st.nextToken());
        }

        // component suffix
        String component = skipComponent ? null : tokenStack.pop();

        nexttok = tokenStack.pop();
        // generation OR format type
        SSTableFormat.Type fmt = SSTableFormat.Type.LEGACY;
        if (!CharMatcher.DIGIT.matchesAllOf(nexttok))
        {
            fmt = SSTableFormat.Type.validate(nexttok);
            nexttok = tokenStack.pop();
        }

        // generation
        int generation = Integer.parseInt(nexttok);

        // version
        nexttok = tokenStack.pop();

        if (!Version.validate(nexttok))
            throw new UnsupportedOperationException("SSTable " + name + " is too old to open.  Upgrade to 2.0 first, and run upgradesstables");

        Version version = fmt.info.getVersion(nexttok);

        // ks/cf names
        String ksname, cfname;
        if (version.hasNewFileName())
        {
            final Pair<String, String> keyspaceAndTable = parseKeyspaceAndTable(new File(directory, name));
            cfname = keyspaceAndTable.left;
            ksname = keyspaceAndTable.right;
        }
        else
        {
            cfname = tokenStack.pop();
            ksname = tokenStack.pop();
        }
        assert tokenStack.isEmpty() : "Invalid file name " + name + " in " + directory;

        return Pair.create(new Descriptor(version, parentDirectory, ksname, cfname, generation, fmt,
                                          // _assume_ version from version
                                          Component.digestFor(version.uncompressedChecksumType())),
                           component);
    }

    public static Pair<String, String> parseKeyspaceAndTable(final File file)
    {
        return parseKeyspaceAndTable(DatabaseDescriptor.getAllDataFileLocations(), file);
    }

    public static Pair<String, String> parseKeyspaceAndTable(final String[] dataLocations, final File file)
    {
        final String filePath = file.getAbsolutePath();

        final Optional<String> dataDir = Stream.of(dataLocations)
                                               .sorted(Comparator.comparingInt(String::length).reversed())
                                               .filter(path -> {
                                                   String dataLocation = path.endsWith(File.separator) ?  path : path + File.separator;
                                                   return filePath.startsWith(dataLocation);
                                               }).findFirst();

        final Function<String, String> enrichWithSeparator = (pattern) ->
        {
            assert pattern != null;

            if (File.separator.equals("\\"))
            {
                return FORWARD_SLASH_PATTERN.matcher(pattern).replaceAll("\\\\");
            }

            return pattern;
        };

        final Function<String, String> parseTableName = (tableDir) ->
        {
            assert tableDir != null;

            if (!tableDir.contains("-"))
            {
                return tableDir;
            }
            else
            {
                return tableDir.split("-")[0];
            }
        };

        final Supplier<Pattern[]> patternsFunction = () ->
        {
            final String prefix = dataDir.orElse("(.*)");

            return new Pattern[] {

            // /some/path/ks/tab/snapshots/snapshot-name/.index/na-1-big-Index.db
            Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/snapshots/(.*)/(\\..*)/(.*)")),
            // /some/path/ks/tab/snapshots/snapshot-name/na-1-big-Index.db
            Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/snapshots/(.*)/(.*)")),

            // /some/path/ks/tab/backups/.index/na-1-big-Index.db
            Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/backups/(\\..*)/(.*)")),
            // /some/path/ks/tab/backups/na-1-big-Index.db
            Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/backups/(.*)")),

            // /some/path/ks/tab/.index/na-1-big-Index.db
            Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/(\\..*)/(.*)")),
            // /some/path/ks/tab/na-1-big-Index.db
            Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/(.*)"))
            };
        };

        final Pattern[] patterns = patternsFunction.get();

        int keyspaceGroup = dataDir.isPresent() ? 1 : 2;
        int tableGroup = dataDir.isPresent() ? 2 : 3;
        int indexGroup = dataDir.isPresent() ? 3 : 4;
        int indexGroupInSnapshots = dataDir.isPresent() ? 4 : 5;
        int indexGroupInBackups = dataDir.isPresent() ? 3 : 4;

        final Matcher indexFileInSnapshotMacher = patterns[0].matcher(filePath);
        if (indexFileInSnapshotMacher.matches()) {
            return Pair.create(indexFileInSnapshotMacher.group(keyspaceGroup),
                               parseTableName.apply(indexFileInSnapshotMacher.group(tableGroup)) + indexFileInSnapshotMacher.group(indexGroupInSnapshots));
        }

        final Matcher snapshotFileMatcher = patterns[1].matcher(filePath);
        if (snapshotFileMatcher.matches()) {
            return Pair.create(snapshotFileMatcher.group(keyspaceGroup),
                               parseTableName.apply(snapshotFileMatcher.group(tableGroup)));
        }

        final Matcher indexFileInBackupMatcher = patterns[2].matcher(filePath);
        if (indexFileInBackupMatcher.matches()) {
            return Pair.create(indexFileInBackupMatcher.group(keyspaceGroup),
                               parseTableName.apply(indexFileInBackupMatcher.group(tableGroup)) + indexFileInBackupMatcher.group(indexGroupInBackups));
        }

        final Matcher backupFileMatcher = patterns[3].matcher(filePath);
        if (backupFileMatcher.matches()) {
            return Pair.create(backupFileMatcher.group(keyspaceGroup),
                               parseTableName.apply(backupFileMatcher.group(tableGroup)));
        }

        final Matcher indexFileMatcher = patterns[4].matcher(filePath);
        if (indexFileMatcher.matches()) {
            return Pair.create(indexFileMatcher.group(keyspaceGroup),
                               parseTableName.apply(indexFileMatcher.group(tableGroup)) + indexFileMatcher.group(indexGroup));
        }

        final Matcher normalFileMatcher = patterns[5].matcher(filePath);
        if (normalFileMatcher.matches()) {
            return Pair.create(normalFileMatcher.group(keyspaceGroup),
                               parseTableName.apply(normalFileMatcher.group(tableGroup)));
        }

        throw new IllegalStateException("Unable to parse keyspace and table from " + filePath);
    }

    public IMetadataSerializer getMetadataSerializer()
    {
        if (version.hasNewStatsFile())
            return new MetadataSerializer();
        else
            return new LegacyMetadataSerializer();
    }

    /**
     * @return true if the current Cassandra version can read the given sstable version
     */
    public boolean isCompatible()
    {
        return version.isCompatible();
    }

    @Override
    public String toString()
    {
        return baseFilename();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof Descriptor))
            return false;
        Descriptor that = (Descriptor)o;
        return that.directory.equals(this.directory)
                       && that.generation == this.generation
                       && that.ksname.equals(this.ksname)
                       && that.cfname.equals(this.cfname)
                       && that.formatType == this.formatType;
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
