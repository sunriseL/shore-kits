#!/usr/bin/perl

use strict;

my @workloads = ('./sim_mix_commands.txt', './q16_commands.txt');
my $qpipe = "./qpipe";
my $qpipe_noreserve = "./qpipe_noreserve";


my $OUTPUT_DIR = "./output/lomond_04.21.2007c";
$| = 1;

my $DRIVER_COMMAND_FILE = "driver_commands.txt";


my @COMMON_CONFIG_VARIABLES = (
    'GLOBAL_OSP',
    'USE_DETERMINISTIC_PREDICATES'
    );

my @RESERVE_CONFIG_VARIABLES =  ();


die "$program not an executable program" if (! -x "$program" );


foreach my $w (@workloads) {
    die "$w not readable file" if ( ! -r $w );
}


foreach my $w (@workloads) {


    # Phase 1: Do not push anything to disk
    if (1) {

        my $common_size  = 1 + $#COMMON_CONFIG_VARIABLES;
        my $common_limit = 2 ** $common_size;
        foreach my $common_config ( 0 .. ($common_limit-1) ) {
            
            my $path_ext = $OUTPUT_DIR."/"."${w}_FLUSH_OFF_RESERVE_ON";
            my @commands = ();
            push(@commands, "config disable FLUSH_TO_DISK_ON_FULL");

            # go through bits
            foreach my $bit_index (0 .. ($common_size-1)) {
                
                my $bit = ($common_config >> $bit_index) & 1;
                my $var = $COMMON_CONFIG_VARIABLES[$bit_index];
                if ($bit) {
                    $path_ext = "${path_ext}_${var}_ON";
                    push(@commands, "config enable $var");
                }
                else {
                    $path_ext = "${path_ext}_${var}_OFF";
                    push(@commands, "config disable $var");
                }
            }

            # run workload commands
            my $wl_command;
            open(WORKLOAD_COMMANDS, $w);
            while ($wl_command = <WORKLOAD_COMMANDS>) {
                push(@commands, $wl_command);
            }
            close(WORKLOAD_COMMANDS);

            # now we have constructed @commands
            open(COMMANDS, ">$DRIVER_COMMAND_FILE");
            foreach my $command (@commands) {
                print COMMANDS "$command \n";
            }
            close(COMMANDS);
            
            system("$qpipe < $DRIVER_COMMAND_FILE | tee $path_ext") == 0
                || die "Failed to run with $path_ext";
        }

    } # endof Phase 1



    # Phase 2: Enable disk flushing
    if (0) {
        
        my @all_config = (@COMMON_CONFIG_VARIABLES, @RESERVE_CONFIG_VARIABLES);
        my $all_size = 1 + $#all_config;
        my $all_limit = 2 ** $all_size;
        foreach my $all_config ( 0 .. ($all_limit-1)) {
            
            my $path_ext = $OUTPUT_DIR."/"."${w}_FLUSH_OFF_RESERVE_OFF";
            my @commands = ();
            push(@commands, "config disable FLUSH_TO_DISK_ON_FULL");
            push(@commands, "config disable RESERVE_WORKERS_BEFORE_DISPATCH");


            # go through bits
            foreach my $bit_index (0 .. ($all_size-1)) {
                
                my $bit = ($all_config >> $bit_index) & 1;
                my $var = $all_config[$bit_index];
                if ($bit) {
                    $path_ext = "${path_ext}_${var}_ON";
                    push(@commands, "config enable $var");
                }
                else {
                    $path_ext = "${path_ext}_${var}_OFF";
                    push(@commands, "config disable $var");
                }
            }

            # run workload commands
            my $wl_command;
            open(WORKLOAD_COMMANDS, $w);
            while ($wl_command = <WORKLOAD_COMMANDS>) {
                push(@commands, $wl_command);
            }
            close(WORKLOAD_COMMANDS);

            # now we have constructed @commands
            open(COMMANDS, ">$DRIVER_COMMAND_FILE");
            foreach my $command (@commands) {
                print COMMANDS "$command \n";
            }
            close(COMMANDS);

            system("$qpipe_noreserve < $DRIVER_COMMAND_FILE | tee $path_ext") == 0
                || die "Failed to run with $path_ext";
        }

    } # endof Phase 2

}