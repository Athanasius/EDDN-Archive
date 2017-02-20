#!/usr/bin/perl -w
# vim: textwidth=0 wrapmargin=0 shiftwidth=2 tabstop=2 softtabstop

use strict;

use Data::Dumper;
use Getopt::Long;
use Date::Manip::Date;
use LWP::UserAgent;
use JSON;

###########################################################################
# Command-line Arguments
###########################################################################
my $date_start_str;
my $date_end_str;
my $archive_type = "journal";
my $part;
my $lastevaluatedtimestamp;
GetOptions (
	"type=s" => \$archive_type,
	"datestart=s" => \$date_start_str,
	"dateend=s" => \$date_end_str,
	"part" => \$part,
	"lastevaluatedtimestamp" => \$lastevaluatedtimestamp
);
if (!defined($date_end_str)) {
	$date_end_str = $date_start_str;
} elsif (defined($part) or defined($lastevaluatedtimestamp)) {
	die("You can only use --part or --lastevaluatedtimestamp if you have no --dateend (can only specify those if you're only fetching a single day).\n");
}
if (!defined($part)) {
	$part = 0;
}

###########################################################################

###########################################################################
###########################################################################
my $data_root = '/mnt/tuesday/data0/eddn-archive/data';
my $base_url = 'https://api.eddn-archive.space/v1/journals/';
my $ua = LWP::UserAgent->new;

#curl -X GET --header 'Accept: application/json' --header 'x-api-key: cqz9odSk4LQYzfGnXQZu2hVH5HGmULb2TIKyCpd2' 'https://api.eddn-archive.space/v1/journals/2016-12-01?limit=10'
my $req = HTTP::Request->new;
$req->method('GET');
$req->header('Accept' => 'application/json');
$req->header('x-api-key' => 'cqz9odSk4LQYzfGnXQZu2hVH5HGmULb2TIKyCpd2');

#################################################################
my $date_start = new Date::Manip::Date;
my $err = $date_start->parse($date_start_str);
if ($err) {
	die("Couldn't parse datestart");
}
my $date_end = new Date::Manip::Date;
$err = $date_end->parse($date_end_str);
if ($err) {
	die("Couldn't parse dateend");
}
my $date_cur = $date_start;
#################################################################
my $backoff_multi = 1.5;
my $backoff_jitter = 10;
my $backoff_sleep_start = 5;
my $backoff_sleep = $backoff_sleep_start;
while ($date_cur->cmp($date_end) != 1) {
	printf STDERR "Running for date: %s\n", $date_cur->printf("%Y-%m-%d");

	my $res;
	my $data;
	while ($part >= 0) {
		my $data_file = $data_root . "/archive-" . $archive_type . "-" . $date_cur->printf("%Y-%m-%d") . "-part" . sprintf("%04d", $part) . ".json";
		if (-e $data_file) {
			printf STDERR "File already exists, so skipping: %s\n", $data_file;
			goto NEXT;
		}
		if (!open(DATA, ">$data_file")) {
			die("Couldn't open output data file: $data_file");
		}
	
		if (defined($LastEvaluatedTimestamp)) {
			$req->uri($base_url . $date_cur->printf("%Y-%m-%d") . "?nexttimestamp=" . $LastEvaluatedTimestamp);
		} else {
			$req->uri($base_url . $date_cur->printf("%Y-%m-%d"));
		}
		#printf STDERR "Request:\n%s\n", Dumper($req); # DEBUG
		while (1) {
			$res = $ua->request($req);
			$data = decode_json($res->content);
			if (! $res->is_success) {
				printf STDERR "!success, json message: %s\n", ${$data}{'message'};
				backoff_sleep($res->status_line);
				next;
			}
			if (defined(${$data}{'__type'}) and ${$data}{'__type'} == "" and defined(${$data}{'message'}) and ${$data}{'message'} =~ /^RequestId: b09caa9f-f78f-11e6-ab78-2bd6b651ae0e Process exited before completing request/) {
			# HTTP Status 400, but still an error
				backoff_sleep(${$data}{'message'});
				next;
			}
			#printf STDERR "Response:\n%s\n", $res->as_string;
			print DATA $res->content;

			# Success, so reduce the next backoff sleep by the multiplier, to a floor of the base value.
			$backoff_sleep = $backoff_sleep / $backoff_multi;
			if ($backoff_sleep < $backoff_sleep_start) {
				$backoff_sleep = $backoff_sleep_start;
			}
			last;
		}
		close(DATA);

		#print Dumper($data);
		if (!defined(${$data}{'LastEvaluatedTimestamp'})) {
			last;
		}
		$LastEvaluatedTimestamp = ${$data}{'LastEvaluatedTimestamp'};
		$part++;
	}

NEXT: {
		$err = $date_cur->next(undef, 0, [0, 0, 0]);
		#printf STDERR "Stepping to date: %s\n", $date_cur->printf("%Y-%m-%d"); # DEBUG
		if ($err) {
			die("Failed to step to next day");
		}
		$part = 0;
		$lastevaluatedtimestamp = undef;
	}
}
#################################################################
###########################################################################
sub backoff_sleep {
	my ($status_line) = @_;
	## Exponential backoff with jitter
	printf STDERR "Error from server '%s\nSleeping for %d seconds...\n", $status_line, $backoff_sleep;
	sleep($backoff_sleep);
	printf STDERR "Trying again...\n";
	$backoff_sleep = $backoff_sleep * $backoff_multi - $backoff_jitter + rand(2 * $backoff_jitter);
}
