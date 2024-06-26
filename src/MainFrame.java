
import java.awt.Color;

import java.sql.DriverManager;
import java.util.Date;

import javax.swing.DefaultListModel;
import javax.swing.JDialog;
import javax.swing.JList;
import javax.swing.JOptionPane;
import static javax.swing.JOptionPane.WARNING_MESSAGE;
import javax.swing.JTextField;

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/GUIForms/JFrame.java to edit this template
 */
/**
 *
 * @author darie
 */
public class MainFrame extends javax.swing.JFrame {

    String origen = null, destino = null;
    int typeO = 0, typeD = 0;
    private Admin admin = null;
    Date fecha = null;

    public MainFrame() {
        initComponents();
        this.setLocationRelativeTo(null);
        this.setResizable(true);

        //admin = new Admin("Prueba2", "Replicar", "2606","1433","user1", "prueba1", "Emilio2606", "Emilio2606", 2);
        //admin = new Admin("postgresql://localhost", "sqlserver://localhost","Prueba2", "pruebita", "2606", "1433", "user1", "prueba1", "Emilio2606", "Emilio2606", 2);
        // admin = new Admin("baseandyor", "baseandyor", "5432", "1434", "postgres", "sa", "1234", "andyor", 1);
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        Tablas = new javax.swing.JDialog();
        jPanel2 = new javax.swing.JPanel();
        jLabel23 = new javax.swing.JLabel();
        jLabel24 = new javax.swing.JLabel();
        jLabel37 = new javax.swing.JLabel();
        bt_right = new javax.swing.JPanel();
        jLabel36 = new javax.swing.JLabel();
        bt_cancelar = new javax.swing.JPanel();
        jLabel30 = new javax.swing.JLabel();
        jScrollPane2 = new javax.swing.JScrollPane();
        jl_tablaR = new javax.swing.JList<>();
        jScrollPane1 = new javax.swing.JScrollPane();
        jl_tablaSR = new javax.swing.JList<>();
        bt_left = new javax.swing.JPanel();
        jLabel31 = new javax.swing.JLabel();
        bt_guardarT = new javax.swing.JPanel();
        jLabel32 = new javax.swing.JLabel();
        FechaActual = new javax.swing.JLabel();
        JLabel = new javax.swing.JLabel();
        jLabel22 = new javax.swing.JLabel();
        Fondo = new javax.swing.JPanel();
        jLabel2 = new javax.swing.JLabel();
        jLabel3 = new javax.swing.JLabel();
        pn_BDdestino = new javax.swing.JPanel();
        jLabel4 = new javax.swing.JLabel();
        tf_instanciaD = new javax.swing.JTextField();
        jLabel5 = new javax.swing.JLabel();
        tf_BDD = new javax.swing.JTextField();
        jLabel6 = new javax.swing.JLabel();
        tf_puertoD = new javax.swing.JTextField();
        jLabel7 = new javax.swing.JLabel();
        tf_usuarioD = new javax.swing.JTextField();
        jLabel8 = new javax.swing.JLabel();
        tf_contraD = new javax.swing.JTextField();
        bt_probarD = new javax.swing.JPanel();
        jLabel9 = new javax.swing.JLabel();
        pn_BDorigen = new javax.swing.JPanel();
        jLabel10 = new javax.swing.JLabel();
        tf_instanciaO = new javax.swing.JTextField();
        jLabel11 = new javax.swing.JLabel();
        tf_BDO = new javax.swing.JTextField();
        jLabel12 = new javax.swing.JLabel();
        tf_puertoO = new javax.swing.JTextField();
        jLabel13 = new javax.swing.JLabel();
        tf_usuarioO = new javax.swing.JTextField();
        jLabel14 = new javax.swing.JLabel();
        tf_contraO = new javax.swing.JTextField();
        bt_probarO = new javax.swing.JPanel();
        jLabel16 = new javax.swing.JLabel();
        bt_guardar = new javax.swing.JPanel();
        jLabel15 = new javax.swing.JLabel();
        jLabel17 = new javax.swing.JLabel();
        jLabel1 = new javax.swing.JLabel();

        Tablas.getContentPane().setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jPanel2.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel23.setBackground(new java.awt.Color(255, 255, 255));
        jLabel23.setFont(new java.awt.Font("Segoe UI", 1, 20)); // NOI18N
        jLabel23.setForeground(new java.awt.Color(255, 255, 255));
        jLabel23.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel23.setText("Replicando");
        jPanel2.add(jLabel23, new org.netbeans.lib.awtextra.AbsoluteConstraints(521, 90, 350, -1));

        jLabel24.setBackground(new java.awt.Color(255, 255, 255));
        jLabel24.setFont(new java.awt.Font("Segoe UI", 1, 20)); // NOI18N
        jLabel24.setForeground(new java.awt.Color(255, 255, 255));
        jLabel24.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel24.setText("Sin Replicar");
        jPanel2.add(jLabel24, new org.netbeans.lib.awtextra.AbsoluteConstraints(32, 90, 350, -1));

        jLabel37.setFont(new java.awt.Font("Segoe UI", 3, 24)); // NOI18N
        jLabel37.setForeground(new java.awt.Color(255, 255, 255));
        jLabel37.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel37.setText("TABLAS BD ORIGEN");
        jPanel2.add(jLabel37, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 30, 900, 30));

        bt_right.setBackground(new java.awt.Color(90, 3, 216));
        bt_right.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                bt_rightMouseClicked(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                bt_rightMouseEntered(evt);
            }
            public void mouseExited(java.awt.event.MouseEvent evt) {
                bt_rightMouseExited(evt);
            }
        });
        bt_right.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel36.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel36.setForeground(new java.awt.Color(255, 255, 255));
        jLabel36.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel36.setText(">>");
        bt_right.add(jLabel36, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 10, 100, -1));

        jPanel2.add(bt_right, new org.netbeans.lib.awtextra.AbsoluteConstraints(400, 260, -1, 40));

        bt_cancelar.setBackground(new java.awt.Color(90, 3, 216));
        bt_cancelar.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                bt_cancelarMouseClicked(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                bt_cancelarMouseEntered(evt);
            }
            public void mouseExited(java.awt.event.MouseEvent evt) {
                bt_cancelarMouseExited(evt);
            }
        });
        bt_cancelar.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel30.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel30.setForeground(new java.awt.Color(255, 255, 255));
        jLabel30.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel30.setText("Cancelar");
        bt_cancelar.add(jLabel30, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 10, 100, -1));

        jPanel2.add(bt_cancelar, new org.netbeans.lib.awtextra.AbsoluteConstraints(300, 530, -1, 40));

        jl_tablaR.setFont(new java.awt.Font("Segoe UI", 0, 14)); // NOI18N
        jl_tablaR.setModel(new DefaultListModel ());
        jScrollPane2.setViewportView(jl_tablaR);

        jPanel2.add(jScrollPane2, new org.netbeans.lib.awtextra.AbsoluteConstraints(550, 140, 290, 350));

        jl_tablaSR.setFont(new java.awt.Font("Segoe UI", 0, 14)); // NOI18N
        jl_tablaSR.setModel(new DefaultListModel ());
        jScrollPane1.setViewportView(jl_tablaSR);

        jPanel2.add(jScrollPane1, new org.netbeans.lib.awtextra.AbsoluteConstraints(60, 140, 290, 350));

        bt_left.setBackground(new java.awt.Color(90, 3, 216));
        bt_left.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                bt_leftMouseClicked(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                bt_leftMouseEntered(evt);
            }
            public void mouseExited(java.awt.event.MouseEvent evt) {
                bt_leftMouseExited(evt);
            }
        });
        bt_left.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel31.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel31.setForeground(new java.awt.Color(255, 255, 255));
        jLabel31.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel31.setText("<<");
        bt_left.add(jLabel31, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 10, 100, -1));

        jPanel2.add(bt_left, new org.netbeans.lib.awtextra.AbsoluteConstraints(400, 320, -1, 40));

        bt_guardarT.setBackground(new java.awt.Color(90, 3, 216));
        bt_guardarT.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                bt_guardarTMouseClicked(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                bt_guardarTMouseEntered(evt);
            }
            public void mouseExited(java.awt.event.MouseEvent evt) {
                bt_guardarTMouseExited(evt);
            }
        });
        bt_guardarT.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel32.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel32.setForeground(new java.awt.Color(255, 255, 255));
        jLabel32.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel32.setText("Guardar");
        jLabel32.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                jLabel32MouseClicked(evt);
            }
        });
        bt_guardarT.add(jLabel32, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 10, 100, -1));

        jPanel2.add(bt_guardarT, new org.netbeans.lib.awtextra.AbsoluteConstraints(140, 530, -1, 40));

        FechaActual.setFont(new java.awt.Font("Segoe UI", 0, 16)); // NOI18N
        FechaActual.setForeground(new java.awt.Color(255, 255, 255));
        FechaActual.setText("Ninguna");
        jPanel2.add(FechaActual, new org.netbeans.lib.awtextra.AbsoluteConstraints(610, 540, -1, -1));

        JLabel.setFont(new java.awt.Font("Segoe UI", 0, 16)); // NOI18N
        JLabel.setForeground(new java.awt.Color(255, 255, 255));
        JLabel.setText("Última modificación:");
        jPanel2.add(JLabel, new org.netbeans.lib.awtextra.AbsoluteConstraints(450, 540, -1, -1));

        jLabel22.setIcon(new javax.swing.ImageIcon(getClass().getResource("/Fotos/fondo 4.png"))); // NOI18N
        jPanel2.add(jLabel22, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 0, -1, -1));

        Tablas.getContentPane().add(jPanel2, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 0, 900, 600));

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        getContentPane().setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        Fondo.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel2.setBackground(new java.awt.Color(255, 255, 255));
        jLabel2.setFont(new java.awt.Font("Segoe UI", 1, 20)); // NOI18N
        jLabel2.setForeground(new java.awt.Color(255, 255, 255));
        jLabel2.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel2.setText("Bases de Datos Destino");
        Fondo.add(jLabel2, new org.netbeans.lib.awtextra.AbsoluteConstraints(461, 90, 410, -1));

        jLabel3.setBackground(new java.awt.Color(255, 255, 255));
        jLabel3.setFont(new java.awt.Font("Segoe UI", 1, 20)); // NOI18N
        jLabel3.setForeground(new java.awt.Color(255, 255, 255));
        jLabel3.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel3.setText("Bases de Datos Origen");
        Fondo.add(jLabel3, new org.netbeans.lib.awtextra.AbsoluteConstraints(32, 90, 410, -1));

        pn_BDdestino.setBackground(new java.awt.Color(245, 247, 251));
        pn_BDdestino.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel4.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel4.setForeground(new java.awt.Color(79, 125, 254));
        jLabel4.setText("Nombre de Instancia");
        pn_BDdestino.add(jLabel4, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 30, -1, -1));
        pn_BDdestino.add(tf_instanciaD, new org.netbeans.lib.awtextra.AbsoluteConstraints(210, 30, 170, -1));

        jLabel5.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel5.setForeground(new java.awt.Color(79, 125, 254));
        jLabel5.setText("Nombre de Base de Datos");
        pn_BDdestino.add(jLabel5, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 80, -1, -1));
        pn_BDdestino.add(tf_BDD, new org.netbeans.lib.awtextra.AbsoluteConstraints(210, 80, 170, -1));

        jLabel6.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel6.setForeground(new java.awt.Color(79, 125, 254));
        jLabel6.setText("Puerto");
        pn_BDdestino.add(jLabel6, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 130, -1, -1));
        pn_BDdestino.add(tf_puertoD, new org.netbeans.lib.awtextra.AbsoluteConstraints(210, 130, 170, -1));

        jLabel7.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel7.setForeground(new java.awt.Color(79, 125, 254));
        jLabel7.setText("Nombre de Usuario");
        pn_BDdestino.add(jLabel7, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 180, -1, -1));
        pn_BDdestino.add(tf_usuarioD, new org.netbeans.lib.awtextra.AbsoluteConstraints(210, 180, 170, -1));

        jLabel8.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel8.setForeground(new java.awt.Color(79, 125, 254));
        jLabel8.setText("Contraseña");
        pn_BDdestino.add(jLabel8, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 230, -1, -1));
        pn_BDdestino.add(tf_contraD, new org.netbeans.lib.awtextra.AbsoluteConstraints(210, 230, 170, -1));

        bt_probarD.setBackground(new java.awt.Color(79, 125, 254));
        bt_probarD.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                bt_probarDMouseClicked(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                bt_probarDMouseEntered(evt);
            }
            public void mouseExited(java.awt.event.MouseEvent evt) {
                bt_probarDMouseExited(evt);
            }
        });
        bt_probarD.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel9.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel9.setForeground(new java.awt.Color(255, 255, 255));
        jLabel9.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel9.setText("Probar");
        bt_probarD.add(jLabel9, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 10, 100, -1));

        pn_BDdestino.add(bt_probarD, new org.netbeans.lib.awtextra.AbsoluteConstraints(160, 290, 100, 40));

        Fondo.add(pn_BDdestino, new org.netbeans.lib.awtextra.AbsoluteConstraints(460, 130, 410, 360));

        pn_BDorigen.setBackground(new java.awt.Color(245, 247, 251));
        pn_BDorigen.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel10.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel10.setForeground(new java.awt.Color(79, 125, 254));
        jLabel10.setText("Nombre de Instancia");
        pn_BDorigen.add(jLabel10, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 30, -1, -1));
        pn_BDorigen.add(tf_instanciaO, new org.netbeans.lib.awtextra.AbsoluteConstraints(210, 30, 170, -1));

        jLabel11.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel11.setForeground(new java.awt.Color(79, 125, 254));
        jLabel11.setText("Nombre de Base de Datos");
        pn_BDorigen.add(jLabel11, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 80, -1, -1));
        pn_BDorigen.add(tf_BDO, new org.netbeans.lib.awtextra.AbsoluteConstraints(210, 80, 170, -1));

        jLabel12.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel12.setForeground(new java.awt.Color(79, 125, 254));
        jLabel12.setText("Puerto");
        pn_BDorigen.add(jLabel12, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 130, -1, -1));
        pn_BDorigen.add(tf_puertoO, new org.netbeans.lib.awtextra.AbsoluteConstraints(210, 130, 170, -1));

        jLabel13.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel13.setForeground(new java.awt.Color(79, 125, 254));
        jLabel13.setText("Nombre de Usuario");
        pn_BDorigen.add(jLabel13, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 180, -1, -1));

        tf_usuarioO.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                tf_usuarioOActionPerformed(evt);
            }
        });
        pn_BDorigen.add(tf_usuarioO, new org.netbeans.lib.awtextra.AbsoluteConstraints(210, 180, 170, -1));

        jLabel14.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel14.setForeground(new java.awt.Color(79, 125, 254));
        jLabel14.setText("Contraseña");
        pn_BDorigen.add(jLabel14, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 230, -1, -1));
        pn_BDorigen.add(tf_contraO, new org.netbeans.lib.awtextra.AbsoluteConstraints(210, 230, 170, -1));

        bt_probarO.setBackground(new java.awt.Color(79, 125, 254));
        bt_probarO.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                bt_probarOMouseClicked(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                bt_probarOMouseEntered(evt);
            }
            public void mouseExited(java.awt.event.MouseEvent evt) {
                bt_probarOMouseExited(evt);
            }
        });
        bt_probarO.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel16.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel16.setForeground(new java.awt.Color(255, 255, 255));
        jLabel16.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel16.setText("Probar");
        bt_probarO.add(jLabel16, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 10, 100, -1));

        pn_BDorigen.add(bt_probarO, new org.netbeans.lib.awtextra.AbsoluteConstraints(160, 290, 100, 40));

        Fondo.add(pn_BDorigen, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 130, 410, 360));

        bt_guardar.setBackground(new java.awt.Color(90, 3, 216));
        bt_guardar.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                bt_guardarMouseClicked(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                bt_guardarMouseEntered(evt);
            }
            public void mouseExited(java.awt.event.MouseEvent evt) {
                bt_guardarMouseExited(evt);
            }
        });
        bt_guardar.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel15.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabel15.setForeground(new java.awt.Color(255, 255, 255));
        jLabel15.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel15.setText("Guardar");
        bt_guardar.add(jLabel15, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 10, 100, -1));

        Fondo.add(bt_guardar, new org.netbeans.lib.awtextra.AbsoluteConstraints(400, 530, -1, 40));

        jLabel17.setFont(new java.awt.Font("Segoe UI", 3, 24)); // NOI18N
        jLabel17.setForeground(new java.awt.Color(255, 255, 255));
        jLabel17.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel17.setText("CONFIGURACIÓN DE BASES DE DATOS ");
        Fondo.add(jLabel17, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 30, 900, 30));

        jLabel1.setIcon(new javax.swing.ImageIcon(getClass().getResource("/Fotos/fondo 4.png"))); // NOI18N
        jLabel1.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                jLabel1MouseClicked(evt);
            }
        });
        Fondo.add(jLabel1, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 0, 900, -1));

        getContentPane().add(Fondo, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 0, 900, 600));

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void bt_probarOMouseEntered(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_probarOMouseEntered
        bt_probarO.setBackground(new Color(68, 212, 229));
    }//GEN-LAST:event_bt_probarOMouseEntered

    private void bt_probarOMouseExited(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_probarOMouseExited
        bt_probarO.setBackground(new Color(79, 125, 254));
    }//GEN-LAST:event_bt_probarOMouseExited

    private void bt_probarDMouseEntered(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_probarDMouseEntered
        bt_probarD.setBackground(new Color(68, 212, 229));
    }//GEN-LAST:event_bt_probarDMouseEntered

    private void bt_probarDMouseExited(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_probarDMouseExited
        bt_probarD.setBackground(new Color(79, 125, 254));
    }//GEN-LAST:event_bt_probarDMouseExited

    private void bt_guardarMouseEntered(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_guardarMouseEntered
        bt_guardar.setBackground(new Color(3, 216, 90));
    }//GEN-LAST:event_bt_guardarMouseEntered

    private void bt_guardarMouseExited(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_guardarMouseExited
        bt_guardar.setBackground(new Color(90, 3, 216));
    }//GEN-LAST:event_bt_guardarMouseExited

    private void bt_guardarMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_guardarMouseClicked

       if (ValidarMotores()) {
            boolean validado = false;
            if (typeO == 2) {//si la base origen es postgres
                admin = new Admin(tf_instanciaO.getText(), tf_instanciaD.getText(), tf_BDO.getText(), tf_BDD.getText(), tf_puertoO.getText(), tf_puertoD.getText(), tf_usuarioO.getText(), tf_usuarioD.getText(), tf_contraO.getText(), tf_contraD.getText(), 2);
                validado = admin.connect();
            } else if (typeO == 1) {//si la base de origen es sqlserver

                admin = new Admin(tf_instanciaD.getText(), tf_instanciaO.getText(), tf_BDD.getText(), tf_BDO.getText(), tf_puertoD.getText(), tf_puertoO.getText(), tf_usuarioD.getText(), tf_usuarioO.getText(), tf_contraD.getText(), tf_contraO.getText(), 1);
                validado =admin.connect();
            } else {
                JOptionPane.showMessageDialog(this, "¡No colocó bien los motores o están mal escritos!", "Warning", WARNING_MESSAGE);
            }

            if(validado){
               
                DefaultListModel tablaSR = (DefaultListModel) jl_tablaSR.getModel();
                tablaSR.clear();
                DefaultListModel tablaR = (DefaultListModel) jl_tablaR.getModel();
                tablaR.clear();
                tablaSR.addAll(admin.getTablasSinReplicar());
                AbrirJD(Tablas);
             
            }else{
                JOptionPane.showMessageDialog(this, "Conexion fallida", "Warning", WARNING_MESSAGE);
            }
            

            
        } else {

            JOptionPane.showMessageDialog(this, "¡No colocó bien los motores o están mal escritos!", "Warning", WARNING_MESSAGE);

        }
    }//GEN-LAST:event_bt_guardarMouseClicked

    private void bt_cancelarMouseEntered(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_cancelarMouseEntered
        bt_cancelar.setBackground(new Color(195, 22, 28));
    }//GEN-LAST:event_bt_cancelarMouseEntered

    private void bt_cancelarMouseExited(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_cancelarMouseExited
        bt_cancelar.setBackground(new Color(90, 3, 216));
    }//GEN-LAST:event_bt_cancelarMouseExited

    private void bt_rightMouseEntered(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_rightMouseEntered
        bt_right.setBackground(new Color(68, 212, 229));
    }//GEN-LAST:event_bt_rightMouseEntered

    private void bt_rightMouseExited(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_rightMouseExited
        bt_right.setBackground(new Color(90, 3, 216));
    }//GEN-LAST:event_bt_rightMouseExited

    private void bt_leftMouseEntered(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_leftMouseEntered
        bt_left.setBackground(new Color(68, 212, 229));
    }//GEN-LAST:event_bt_leftMouseEntered

    private void bt_leftMouseExited(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_leftMouseExited
        bt_left.setBackground(new Color(90, 3, 216));
    }//GEN-LAST:event_bt_leftMouseExited

    private void bt_guardarTMouseEntered(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_guardarTMouseEntered
        bt_guardarT.setBackground(new Color(3, 216, 90));
    }//GEN-LAST:event_bt_guardarTMouseEntered

    private void bt_guardarTMouseExited(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_guardarTMouseExited
        bt_guardarT.setBackground(new Color(90, 3, 216));
    }//GEN-LAST:event_bt_guardarTMouseExited

    private void bt_cancelarMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_cancelarMouseClicked
        Tablas.setVisible(false);
    }//GEN-LAST:event_bt_cancelarMouseClicked

    private void bt_probarOMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_probarOMouseClicked
//        int num2 = 0;
//        if (tf_instanciaO.getText().contains("sqlserver")) {
//            num2 = 1;
//        } else if (tf_instanciaO.getText().contains("postgresql")) {
//            num2 = 2;
//        }
        if (!ClasificarType(1)) {
            JOptionPane.showMessageDialog(this, "¡Error en el nombre de instancia!", "Warning", WARNING_MESSAGE);
        } else {
            if (CheckingTextFields(tf_BDO.getText(), tf_puertoO.getText(), tf_usuarioO.getText(), tf_contraO.getText())) {//revisa si hay campos nulos
                if (test(tf_instanciaO.getText(), tf_BDO.getText(), tf_puertoO.getText(), tf_usuarioO.getText(), tf_contraO.getText(), typeO)) {
                    JOptionPane.showMessageDialog(this, "¡Prueba éxitosa!");
                } else {
                    JOptionPane.showMessageDialog(this, "¡Prueba fallida!");
                }
            }else {//hay campos nulos
                JOptionPane.showMessageDialog(this, "¡No se puede realizar la prueba! Hay al menos un campo sin texto.", "Warning", WARNING_MESSAGE);
            }
        }
    }//GEN-LAST:event_bt_probarOMouseClicked

    public boolean test(String instancia, String base, String puerto, String usuario, String pw, int type) {
        try {
            if (type == 1) {//sqlserver
                DriverManager.getConnection("jdbc:" + instancia + ":" + puerto + ";database=" + base + ";user=" + usuario + ";password=" + pw + ";encrypt=true;" + "trustServerCertificate=true;" + "loginTimeout=30;");
            } else {
                DriverManager.getConnection("jdbc:" + instancia + ":" + puerto + "/" + base, usuario, pw);//postgres

            }
            return true;
        } catch (Exception e) {
            return false;
        }

    }

    private void bt_probarDMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_probarDMouseClicked
//        int num2 = 0;
//        if (tf_instanciaD.getText().contains("sqlserver")) {
//            num2 = 1;
//        } else if (tf_instanciaD.getText().contains("postgresql")) {
//            num2 = 2;
//        }

        if (!ClasificarType(2)) {
            JOptionPane.showMessageDialog(this, "¡Error en el nombre de instancia!", "Warning", WARNING_MESSAGE);
        } else {
            if (CheckingTextFields(tf_BDD.getText(), tf_puertoD.getText(), tf_usuarioD.getText(), tf_contraD.getText())) {//revisa si hay campos nulos
                if (test(tf_instanciaD.getText(), tf_BDD.getText(), tf_puertoD.getText(), tf_usuarioD.getText(), tf_contraD.getText(), typeD)) {
                    JOptionPane.showMessageDialog(this, "¡Prueba éxitosa!");
                } else {
                    JOptionPane.showMessageDialog(this, "¡Prueba fallida!");
                }
            }else {//hay campos nulos
                JOptionPane.showMessageDialog(this, "¡No se puede realizar la prueba! Hay al menos un campo sin texto.", "Warning", WARNING_MESSAGE);
            }
        }
    }//GEN-LAST:event_bt_probarDMouseClicked

    private void bt_rightMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_rightMouseClicked
        DefaultListModel tablaSR = (DefaultListModel) jl_tablaSR.getModel();
        DefaultListModel tablaR = (DefaultListModel) jl_tablaR.getModel();
        
        int index = jl_tablaSR.getSelectedIndex();
        if (index > -1) {
            admin.getTablasSinReplicar().remove(tablaSR.get(index).toString());
            admin.getTablasReplicadas().add(tablaSR.get(index).toString());
            tablaR.addElement(tablaSR.get(index));
            tablaSR.remove(index);
        } else {
            JOptionPane.showMessageDialog(this, "¡No hay ninguna tabla seleccionada!", "Warning", WARNING_MESSAGE);
        }
    }//GEN-LAST:event_bt_rightMouseClicked

    private void bt_leftMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_leftMouseClicked
        DefaultListModel tablaSR = (DefaultListModel) jl_tablaSR.getModel();
        DefaultListModel tablaR = (DefaultListModel) jl_tablaR.getModel();

        int index = jl_tablaR.getSelectedIndex();
        if (index > -1) {
            admin.getTablasSinReplicar().add(tablaR.get(index).toString());
            admin.getTablasReplicadas().remove(tablaR.get(index).toString());
            tablaSR.addElement(tablaR.get(index));
            tablaR.remove(index);
        } else {
            JOptionPane.showMessageDialog(this, "¡No hay ninguna tabla seleccionada!", "Warning", WARNING_MESSAGE);
        }
    }//GEN-LAST:event_bt_leftMouseClicked

    private void bt_guardarTMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_bt_guardarTMouseClicked
        boolean funciono = false;

        if (admin.getCual() == 1) {
            funciono = admin.replicarServerToPostgre(fecha);
            //el null se va a cambiar por la date actual, si es la primera vez q se usa si va a ser null
        } else {
            funciono = admin.replicarPostgreToServer(fecha);
            //el null se va a cambiar por la date actual, si es la primera vez q se usa si va a ser null
        }
        
        if(funciono){
            JOptionPane.showMessageDialog(this, "La base se replico exitosamente");
            fecha = new Date();
            FechaActual.setText(fecha.toString());
        }else{
            JOptionPane.showMessageDialog(this, "La base no se replico");
        }
        
        /*
        //actualizar la lista de tablas sin replicar
        DefaultListModel tablaSR = (DefaultListModel) jl_tablaSR.getModel();

        admin.getTablasSinReplicar().clear();
        for (int i = 0; i < tablaSR.getSize(); i++) {
            admin.getTablasSinReplicar().add(tablaSR.getElementAt(i).toString());
        }

        //actualizar la lista de tablas replicadas
        DefaultListModel tablaR = (DefaultListModel) jl_tablaR.getModel();

        for (int i = 0; i < tablaR.getSize(); i++) {
            admin.getTablasReplicadas().add(tablaR.getElementAt(i).toString());
        }

        for (int i = 0; i < admin.getTablasReplicadas().size(); i++) {
            System.out.println(admin.getTablasReplicadas().get(i));
        }
        */
    }//GEN-LAST:event_bt_guardarTMouseClicked

    private void tf_usuarioOActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_tf_usuarioOActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_tf_usuarioOActionPerformed

    private void jLabel1MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_jLabel1MouseClicked
        // TODO add your handling code here:
    }//GEN-LAST:event_jLabel1MouseClicked

    private void jLabel32MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_jLabel32MouseClicked
        // TODO add your handling code here:
    }//GEN-LAST:event_jLabel32MouseClicked

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(MainFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(MainFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(MainFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(MainFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new MainFrame().setVisible(true);
            }
        });
    }

    public void FormatearLista(JList lista) {
        DefaultListModel modelo = (DefaultListModel) lista.getModel();
        modelo.removeAllElements();
    }

    public boolean CheckingTextFields(String db, String port, String username, String pw) {
        if (db.isEmpty()) {
            return false;
        } else if (port.isEmpty()) {
            return false;
        } else if (username.isEmpty()) {
            return false;
        } else if (pw.isEmpty()) {
            return false;
        }

        return true;
    }

    public void AbrirJD(JDialog JD) {
        JD.setModal(true);
        JD.pack();
        JD.setLocationRelativeTo(null);
        JD.setResizable(false);
        JD.setVisible(true);
    }

    public boolean ClasificarType(int clasif) {
        if (clasif == 1) {//origen
            if (tf_instanciaO.getText().contains("postgresql")) {
                origen = "postgresql";
                typeO = 2;
                return true;
            } else if (tf_instanciaO.getText().contains("sqlserver")) {
                origen = "sqlserver";
                typeO = 1;
                return true;
            } else {
                return false;
            }

        } else {//destino
            if (tf_instanciaD.getText().contains("postgresql")) {
                destino = "postgresql";
                typeD = 2;
                return true;
            } else if (tf_instanciaD.getText().contains("sqlserver")) {
                destino = "sqlserver";
                typeD = 1;
                return true;
            } else {
                return false;
            }
        }
    }

    public boolean ValidarMotores() {//valida si hay un motor diferente en cada base
        boolean flag;

        //validar motor de origen
        flag = ClasificarType(1);

        //validar motor de destino
        flag = ClasificarType(2);

        if (origen == null || destino == null) {
            
            flag = false;
        } else if ("postgresql".equals(origen) && "postgresql".equals(destino)) {
     
            flag = false;
        } else if ("sqlserver".equals(origen) && "sqlserver".equals(destino)) {
        
            flag = false;
        }

        return flag;
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JLabel FechaActual;
    private javax.swing.JPanel Fondo;
    private javax.swing.JLabel JLabel;
    private javax.swing.JDialog Tablas;
    private javax.swing.JPanel bt_cancelar;
    private javax.swing.JPanel bt_guardar;
    private javax.swing.JPanel bt_guardarT;
    private javax.swing.JPanel bt_left;
    private javax.swing.JPanel bt_probarD;
    private javax.swing.JPanel bt_probarO;
    private javax.swing.JPanel bt_right;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel10;
    private javax.swing.JLabel jLabel11;
    private javax.swing.JLabel jLabel12;
    private javax.swing.JLabel jLabel13;
    private javax.swing.JLabel jLabel14;
    private javax.swing.JLabel jLabel15;
    private javax.swing.JLabel jLabel16;
    private javax.swing.JLabel jLabel17;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JLabel jLabel22;
    private javax.swing.JLabel jLabel23;
    private javax.swing.JLabel jLabel24;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabel30;
    private javax.swing.JLabel jLabel31;
    private javax.swing.JLabel jLabel32;
    private javax.swing.JLabel jLabel36;
    private javax.swing.JLabel jLabel37;
    private javax.swing.JLabel jLabel4;
    private javax.swing.JLabel jLabel5;
    private javax.swing.JLabel jLabel6;
    private javax.swing.JLabel jLabel7;
    private javax.swing.JLabel jLabel8;
    private javax.swing.JLabel jLabel9;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JList<String> jl_tablaR;
    private javax.swing.JList<String> jl_tablaSR;
    private javax.swing.JPanel pn_BDdestino;
    private javax.swing.JPanel pn_BDorigen;
    private javax.swing.JTextField tf_BDD;
    private javax.swing.JTextField tf_BDO;
    private javax.swing.JTextField tf_contraD;
    private javax.swing.JTextField tf_contraO;
    private javax.swing.JTextField tf_instanciaD;
    private javax.swing.JTextField tf_instanciaO;
    private javax.swing.JTextField tf_puertoD;
    private javax.swing.JTextField tf_puertoO;
    private javax.swing.JTextField tf_usuarioD;
    private javax.swing.JTextField tf_usuarioO;
    // End of variables declaration//GEN-END:variables
}
