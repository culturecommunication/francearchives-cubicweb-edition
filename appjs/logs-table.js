/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2019
 * Contact http://www.logilab.fr -- mailto:contact@logilab.fr
 *
 * This software is governed by the CeCILL-C license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-C
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systemsand/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-C license and that you accept its terms.
 */
import {render} from 'react-dom';
import {Component, createElement as ce} from 'react';

import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';


function caretRender(direction, fieldName) {
    const carets = [];
    let selected = false;
    if (direction === 'asc') {
        selected = true;
        carets.push(ce('i', {className: 'fa fa-caret-up'}));
    } else if (direction === 'desc') {
        selected = true;
        carets.push(ce('i', {className: 'fa fa-caret-down'}));
    } else {
        carets.push(
            ce('i', {className: 'fa fa-caret-up'}),
            ce('i', {className: 'fa fa-caret-down'})
        );
    }
    return ce('span', {
        className: `circulartable_caret ${selected ? 'circulartable_caret-selected' : ''}`
    }, ...carets);
}



class LogsTable extends Component {

    constructor(props) {
        super(props);
        this.state = {selectedSeverity: this.props.all_levels_default,
                      selectedLogs: this.props.logs};
        this.updateSeverity = this.updateSeverity.bind(this);
        this.buildOptions = this.buildOptions.bind(this);
    }

    updateSeverity(ev) {
        ev.preventDefault();
        const selectedSeverity = ev.target.value;
        this.setState({
            selectedSeverity,
            selectedLogs: (selectedSeverity === this.props.all_levels_default ? this.props.logs :
                this.props.logs.filter(d => d.severity === selectedSeverity))
        });
    }

    buildOptions() {
        const all_levels = [this.props.all_levels_default].concat(
            Array.from(new Set(this.props.logs.map(value => value.severity))));
        return all_levels.map(
            (b, idx) => ce('option', {key: `bfield-${idx}`, value: b}, b)
        );
    }

    render() {
        const {selectedSeverity, selectedLogs} = this.state,
              dataLength = selectedLogs.length;
        return ce(
            'div', null,
            ce('span', null, 'Sélectionner un niveau : '),
            ce(
                'select', {value: selectedSeverity, onChange: this.updateSeverity},
                this.buildOptions()
            ),
            ce(
                BootstrapTable, {
                    data: selectedLogs,
                    striped: true,
                    hover: true,
                    pagination: true,
                    search: true,
                    searchPlaceholder: 'rechercher',
                    options: {
                        defaultSortOrder: 'asc',
                        defaultSortName: 'date',
                        sizePerPage: dataLength < 20 ? dataLength : 20,
                        sizePerPageList: [
                            {
                                text: '20',
                                value: 20
                            },
                            {
                                text: '50',
                                value: 50
                            },
                            {
                                text: '100',
                                value: 100
                            },
                            {
                                text: `tout (${selectedLogs.length})`,
                                value: dataLength
                            },
                        ]
                    }
                },
                //ce(TableHeaderColumn, {isKey: true, dataField: 'eid', hidden: true}),
                ce(TableHeaderColumn, {isKey: true, dataField: 'severity', with: '20%', dataSort: true, caretRender}, 'Sévérité'),
                ce(TableHeaderColumn, {
                    dataField: 'date',
                    with: '5%',
                    dataSort: true,
                    caretRender,
                    dataFormat(cell) {
                        if (!cell) {
                            return;
                        }
                        const [date, time] = cell.split(' '),
                              [year, month, day] = date.split('-');
                        return `${day}/${month}/${year} ${time}`;
                    }}, 'Date'),
                ce(TableHeaderColumn, {dataField: 'time', width: '20%', dataSort: true, caretRender}, 'Temps'),
                ce(TableHeaderColumn, {dataField: 'message',  width: '50%'}, 'Message'),
            ),
        );
    }

}

const target = document.getElementById('logs-table-container');
if (target !== null) {
    render(
        ce(LogsTable, {logs: logs, all_levels_default: '-- tous --'}),
        target
    );
}
