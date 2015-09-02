using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

using CommonLib;
using WorkerLib;

namespace PuppetMaster {

    public partial class PuppetMasterForm : Form {

        private PuppetMasterConsole console;

        private bool scriptChanged = false;

        private bool clearWorkersFirst = false;

        public PuppetMasterForm(PuppetMasterConsole pm) {
            console = pm;
            InitializeComponent();
        }

        private delegate void CreateWorkerDelegate(int id);

        private delegate void ChangeStatusDelegate(int id, string status);

        public void CreateWorker(int id) {
            if (console.IsFormActive) {
                if (clearWorkersFirst) {
                    ClearWorkerList();
                    clearWorkersFirst = false;
                }

                ListViewItem item = workersList.Items.Add("" + id);
                item.SubItems.Add("Running").Tag = "status";
                item.SubItems.Add("Slow").Tag = "sloww";
                item.SubItems.Add("Freeze").Tag = "freezew";
                item.SubItems.Add("Unfreeze").Tag = "unfreezew";
                item.SubItems.Add("Kill").Tag = "kill";
            }
        }

        public void ChangeStatus(int id, string status) {
            if (console.IsFormActive) {
                foreach (ListViewItem item in workersList.Items) {
                    if (item.Text.Equals("" + id)) {
                        item.SubItems[1].Text = status;
                    }
                }
            }
        }

        public void ClearWorkerList() {
            workersList.Items.Clear();
        }

        public void PuppetMasterChanged(IList<IWorker> workers, int id) {
            clearWorkersFirst = true;
            foreach (IWorker w in workers) {
                this.BeginInvoke(new CreateWorkerDelegate(CreateWorker), w.GetId());
                this.BeginInvoke(new ChangeStatusDelegate(ChangeStatus), w.GetId(), w.GetStatus());
            }
        }

        public void PuppetMasterStatus(int id, string status) {
            this.BeginInvoke(new ChangeStatusDelegate(ChangeStatus), id, status);
        }

        private void onFormLoad(object sender, EventArgs e) {
            console.IsFormActive = true;
            workersList.View = View.Details;
            workersList.FullRowSelect = true;
            ListViewExtender extender = new ListViewExtender(workersList);

            // Columns with buttons, TODO: change OnButtonActionClick
            ListViewButtonColumn slowColumn = new ListViewButtonColumn(2);
            slowColumn.Click += OnButtonActionClick;
            slowColumn.FixedWidth = true;
            extender.AddColumn(slowColumn);

            ListViewButtonColumn freezeColumn = new ListViewButtonColumn(3);
            freezeColumn.Click += OnButtonActionClick;
            freezeColumn.FixedWidth = true;
            extender.AddColumn(freezeColumn);

            ListViewButtonColumn unfreezeColumn = new ListViewButtonColumn(4);
            unfreezeColumn.Click += OnButtonActionClick;
            unfreezeColumn.FixedWidth = true;
            extender.AddColumn(unfreezeColumn);

            ListViewButtonColumn killColumn = new ListViewButtonColumn(5);
            killColumn.Click += OnButtonActionClick;
            killColumn.FixedWidth = true;
            extender.AddColumn(killColumn);

            // waits for service to start (first time)
            while (console.Service == null) ;

            console.Service.OnChange += PuppetMasterChanged;
            Worker.OnStatus += PuppetMasterStatus;

            PuppetMasterChanged(console.Service.Workers, 0);

            this.Text = console.PuppetMasterURL;
        }


        private void OnButtonActionClick(object sender, ListViewColumnMouseEventArgs e) {
            string command = "";
            string seconds = "";

            if (e.SubItem.Tag.Equals("sloww")) {
                seconds = DialogCombo("Insert the time in seconds");
            }

            command = e.SubItem.Tag + " " + e.Item.Text + " " + seconds;

            console.SaveScript(command);

            new Thread(() => {
                console.RunScript();
            }).Start();
        }

        private void OnClosing(object sender, FormClosingEventArgs e) {
            console.IsFormActive = false;
            console.Service.OnChange -= PuppetMasterChanged;
            Worker.OnStatus -= PuppetMasterStatus;
        }

        private void OnScriptChanged(object sender, EventArgs e) {
            scriptChanged = true;
            stepButton.Text = "Save";
        }

        private void stepButton_Click(object sender, EventArgs e) {
            if (scriptChanged) {
                console.SaveScript(consoleTextBox.Text);
                scriptChanged = false;
                stepButton.Text = "Step";
            } else {
                new Thread(() => {
                    console.StepScript();
                }).Start();

            }
        }

        private void runButton_Click(object sender, EventArgs e) {
            console.SaveScript(consoleTextBox.Text);
            new Thread(() => {
                console.RunScript();
            }).Start();
        }

        private void openToolStripMenuItem_Click(object sender, EventArgs e) {
            OpenFileDialog openFileDialog1 = new OpenFileDialog();

            openFileDialog1.InitialDirectory = "c:\\";
            openFileDialog1.Filter = "txt files (*.txt)|*.txt|All files (*.*)|*.*";
            openFileDialog1.FilterIndex = 2;
            openFileDialog1.RestoreDirectory = true;

            if (openFileDialog1.ShowDialog() == DialogResult.OK) {
                try {
                    consoleTextBox.Text = File.ReadAllText(openFileDialog1.FileName);
                } catch (Exception ex) {
                    MessageBox.Show("Error: Could not read file from disk. Original error: " + ex.Message);
                }
            }
        }

        private void saveScriptToolStripMenuItem_Click(object sender, EventArgs e) {
            SaveFileDialog saveFileDialog1 = new SaveFileDialog();

            saveFileDialog1.InitialDirectory = "c:\\";
            saveFileDialog1.Filter = "txt files (*.txt)|*.txt|All files (*.*)|*.*";
            saveFileDialog1.FilterIndex = 2;
            saveFileDialog1.RestoreDirectory = true;

            if (saveFileDialog1.ShowDialog() == DialogResult.OK) {
                try {
                    File.WriteAllText(saveFileDialog1.FileName, consoleTextBox.Text);
                } catch (Exception ex) {
                    MessageBox.Show("Error: Could not write file to disk. Original error: " + ex.Message);
                }
            }
        }

        public static string DialogCombo(string text) {
            AskInputForm prompt = new AskInputForm();
            prompt.Text = text;
            prompt.ShowDialog();

            return prompt.ReturnValue;
        }
    }
}
