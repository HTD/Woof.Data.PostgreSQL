using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Woof.SystemEx;


namespace Woof.DeploymentEx {

    /// <summary>
    /// Automatic PostgreSQL client installer.
    /// </summary>
    public class PostgresClientInstaller : IDisposable {

        #region Configuration

        static readonly string BinariesDownloadURL = "https://www.enterprisedb.com/download-postgresql-binaries";
        static readonly string WindowsX64RelPattern =
            Target.IsX86
                ? "/postgresql-*-binaries-win32"
                : "/postgresql-*-binaries-win64";
        static readonly string ZipArchivePattern =
            Target.IsX86
                ? "https://get.enterprisedb.com/postgresql/postgresql-*-windows-binaries.zip"
                : "https://get.enterprisedb.com/postgresql/postgresql-*-windows-x64-binaries.zip";
        static readonly string BinDirectory = "pgsql\\bin";
        static readonly string[] ClientFiles = new string[] {
            "iconv.dll", "libeay32.dll", "libiconv-2.dll", "libintl-8.dll", "libpq.dll", "msvcr120.dll", "ssleay32.dll", "zlib1.dll",
            "pg_dump.exe", "pg_dumpall.exe", "pg_restore.exe", "psql.exe"
        };
        static readonly string TargetPath = Path.Combine(Target.GetProgramFilesDirectory(Target.Auto), "PGSQL");
        
        private readonly TextWriter MessageOutput;

        #endregion

        #region Events

        /// <summary>
        /// Occurs when the download link is found.
        /// </summary>
        public event EventHandler DownloadLinkFound;

        /// <summary>
        /// Occurs when the download process is started.
        /// </summary>
        public event EventHandler DownloadStarted;

        /// <summary>
        /// Occurs when the download progress advanced by at least 1%.
        /// </summary>
        public event EventHandler<PercentEventArgs> DownloadProgressChanged;

        /// <summary>
        /// Occurs when the download has completed.
        /// </summary>
        public event EventHandler DownloadCompleted;

        /// <summary>
        /// Occurs when a file extraction is started.
        /// </summary>
        public event EventHandler<ItemEventArgs<ZipArchiveEntry>> ExtractingFile;

        /// <summary>
        /// Occurs when a file extraction is completed.
        /// </summary>
        public event EventHandler ExtractingFileDone;

        #endregion

        #region Properties

        /// <summary>
        /// Gets a value indicating whether PostgreSQL client is installed.
        /// </summary>
        public static bool IsInstalled => PathTools.IsFileAcessibleInPath("psql.exe");

        /// <summary>
        /// Gets a PostgreSQL client binaries directory if available.
        /// </summary>
        public static string Bin => IsInstalled ? Path.GetDirectoryName(PathTools.GetFullPath("psql.exe")) : null;

        #endregion

        /// <summary>
        /// Installs the latest version of PostgreSQL client binaries
        /// from the EDB website to user or system program files directory
        /// and adds it to the environment path.
        /// Invoke as user to make per-user installation, invoke as Administrator to make per-machine installation.
        /// Compile with "Prefer 32-bit" to install 32-bit version, uncheck "Prefer 32-bit" to install x64 version.
        /// Blocks current thread until completed.
        /// </summary>
        /// <param name="messageOutput">
        /// Optional text writer to write diagnostic messages to, like <see cref="Console.Out"/>.
        /// Do not provide if you intend to use events instead.
        /// </param>
        /// <returns>True if successfull.</returns>
        public static bool Install(TextWriter messageOutput = null) {
            using (var installer = new PostgresClientInstaller(messageOutput)) return installer.Install();
        }

        /// <summary>
        /// Ensures the PostgreSQL client binaries are installed.
        /// If the aren't they will be.
        /// </summary>
        /// <param name="messageOutput">
        /// Optional text writer to write diagnostic messages to, like <see cref="Console.Out"/>.
        /// Do not provide if you intend to use events instead.
        /// </param>
        /// <returns>True if PostgreSQL client binaries are installed correctly.</returns>
        public static bool EnsureInstalled(TextWriter messageOutput = null) {
            if (PathTools.IsFileAcessibleInPath("psql.exe")) return true;
            using (var installer = new PostgresClientInstaller(messageOutput)) return installer.Install();
        }

        /// <summary>
        /// Creates new PostgreSQL installer with optional message output.
        /// </summary>
        /// <param name="messageOutput">
        /// Optional text writer to write diagnostic messages to, like <see cref="Console.Out"/>.
        /// Do not provide if you intend to use events instead.
        /// </param>
        public PostgresClientInstaller(TextWriter messageOutput = null) => MessageOutput = messageOutput;

        /// <summary>
        /// Installs the latest version of PostgreSQL client binaries
        /// from the EDB website to user or system program files directory
        /// and adds it to the environment path.
        /// Invoke as user to make per-user installation, invoke as Administrator to make per-machine installation.
        /// Compile with "Prefer 32-bit" to install 32-bit version, uncheck "Prefer 32-bit" to install x64 version.
        /// Blocks current thread until completed.
        /// </summary>
        /// <returns>True if successfull.</returns>
        public bool Install() {
            Directory.CreateDirectory(TargetPath);
            if (Download(TargetPath)) {
                PathTools.AddToPath(TargetPath, Target.Auto);
                return true;
            }
            else return false;
        }

        /// <summary>
        /// Downloads and extracts the latest Windows client binaries.
        /// Blocks the current thread until completed.
        /// </summary>
        /// <param name="targetPath"></param>
        /// <returns></returns>
        private bool Download(string targetPath) {
            var t0 = DateTime.Now;
            MessageOutput?.WriteLine("Starting PostgeSQL client installer...");
            var edb = new Uri(BinariesDownloadURL);
            var link = LinkTool.FetchLastVersionLink(edb, WindowsX64RelPattern, ZipArchivePattern);
            if (link == null) {
                MessageOutput?.WriteLine("Download error.");
                return false;
            }
            if (MessageOutput != null) {
                DownloadLinkFound += (s, e) => MessageOutput.WriteLine($"Download link found...");
                DownloadProgressChanged += (s, e) => MessageOutput.Write(".");
                DownloadStarted += (s, e) => MessageOutput.WriteLine($"Downloading {link}...");
                ExtractingFile += (s, e) => MessageOutput.Write($"Extracting file {e.Item.Name}...");
                ExtractingFileDone += (s, e) => MessageOutput.WriteLine("OK.");
                DownloadCompleted += (s, e) => MessageOutput.WriteLine();
            }
            DownloadLinkFound?.Invoke(link, EventArgs.Empty);
            using (var download = new Download(link)) {
                DownloadStarted?.Invoke(this, EventArgs.Empty);
                download.DownloadCompleted += (s, e) => DownloadCompleted?.Invoke(this, EventArgs.Empty);
                download.DownloadProgressChanged += (s, e) => DownloadProgressChanged?.Invoke(this, e);
                var stream = download.GetStream();
                if (stream == null) return false;
                using (var archive = new ZipArchive(stream, ZipArchiveMode.Read)) {
                    foreach (var entry in archive.Entries.Where(i => Path.GetDirectoryName(i.FullName) == BinDirectory && ClientFiles.Contains(i.Name))) {
                        ExtractingFile?.Invoke(archive, new ItemEventArgs<ZipArchiveEntry>(entry));
                        entry.ExtractToFile(Path.Combine(targetPath, entry.Name), true);
                        ExtractingFileDone?.Invoke(archive, EventArgs.Empty);
                    }
                }
            }
            var t = DateTime.Now - t0;
            MessageOutput?.WriteLine($"Completed in {t.TotalSeconds:0.000}s.");
            return true;
        }

        /// <summary>
        /// Releases memory allocated by the download.
        /// </summary>
        public void Dispose() => GC.Collect();

    }


}